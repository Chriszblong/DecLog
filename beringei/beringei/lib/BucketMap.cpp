/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BucketMap.h"

#include "BucketLogWriter.h"
#include "BucketUtils.h"
#include "DataBlockReader.h"
#include "DataLog.h"
#include "GorillaStatsManager.h"
#include "GorillaTimeConstants.h"
#include "TimeSeries.h"

DECLARE_int32(using_libpmem_or_libpmem2);
DECLARE_int32(max_allowed_LSN);
DECLARE_int64(data_point_nonexistent);
DECLARE_int64(tombstone_setted);
DECLARE_int64(data_point_in_updatedDataMap);
DECLARE_int32(log_writer_threads);
DECLARE_int32(is_checkpoint);

DEFINE_int32(
    is_transaction,
    1,
    "Whether the database is running for transaction.");

DEFINE_int64(
    threads_for_recovery, 
    1,
    "Number of threads for recovery.");

DEFINE_int64(
    logs_queue_size,
    1000,
    "The size of the log queue of threads for recovery.");

DEFINE_int32(
    data_point_queue_size,
    1000,
    "The size of the qeueue that holds the data points in memory before they "
    "can be handled. This queue is only used when shards are being added.");

DEFINE_int64(
    missing_logs_threshold_secs,
    600, // 10 minute default
    "Count gaps longer than this as holes in the log files.");

DECLARE_int32(log_flag_checkpoint);

namespace facebook {
namespace gorilla {

DECLARE_int32(log_flag_insertion);
DECLARE_int32(log_flag_deletion);
DECLARE_int32(log_flag_update);

DECLARE_int32(group_commit_numbers);
DECLARE_int32(number_of_logging_queues);

// When performing initial insertion, add this much buffer to the vector
// on each resize.
const int kRowsAtATime = 10000;

extern const uint32_t kMaxPageCount;

extern const size_t kLargeFileBuffer;

static const int log_insertion = 1;
static const int log_deletion = 3;
static const int log_update = 2;

static std::vector<bool> stopInsertThreads(8, false);
static std::vector<bool> isInsertThreadRunnings(8, false);

static const std::string kMsPerKeyListRead = ".ms_per_key_list_read";
static const std::string kMsPerLogFilesRead = ".ms_per_log_files_read";
static const std::string kMsPerBlockFileRead = ".ms_per_block_file_read";
static const std::string kMsPerQueueProcessing = ".ms_per_queue_processing";
static const std::string kDataPointQueueDropped = ".data_point_queue_dropped";
static const std::string kCorruptKeyFiles = ".corrupt_key_files";
static const std::string kCorruptLogFiles = ".corrupt_log_files";
static const std::string kUnknownKeysInLogFiles = ".unknown_keys_in_log_files";
static const std::string kUnknownKeysInBlockMetadataFiles =
    ".unknown_keys_in_block_metadata_files";
static const std::string kDataHoles = ".missing_blocks_and_logs";
static const std::string kMissingLogs = ".missing_seconds_of_log_data";
static const std::string kDeletionRaces = ".key_deletion_failures";
static const std::string kDuplicateKeys = ".duplicate_keys_in_key_list";

static const size_t kMaxAllowedKeyLength = 400;

static int16_t kInstagramCategoryId = 271;

const int BucketMap::kNotOwned = -1;

DECLARE_int32(max_allowed_timeseries_id);

BucketMap::BucketMap(
    uint8_t buckets,
    uint64_t windowSize,
    int shardId,
    const std::string& dataDirectory,
    std::shared_ptr<KeyListWriter> keyWriter,
    std::shared_ptr<BucketLogWriter> logWriter,
    BucketMap::State state)
    : n_(buckets),
      windowSize_(windowSize),
      reliableDataStartTime_(0),
      lock_(),
      tableSize_(0),
      storage_(buckets, shardId, dataDirectory),
      state_(state),
      shardId_(shardId),
      dataDirectory_(dataDirectory),
      keyWriter_(keyWriter),
      logWriter_(logWriter),
      lastFinalizedBucket_(0) {}

// Insert the given data point, creating a new row if necessary.
// Returns the number of new rows created (0 or 1) and the number of
// data points successfully inserted (0 or 1) as a pair of ints.
// Returns {kNotOwned,kNotOwned} if this map is currenly not owned.
std::pair<int, int> BucketMap::put(
    const std::string& key,
    const TimeValuePair& value,
    uint16_t category,
    bool skipStateCheck) {
  State state;
  uint32_t id;
  auto existingItem = getInternal(key, state, id);

  // State check can only skipped when processing data points from the
  // queue. Data points that come in externally during processing will
  // still be queued.
  if (skipStateCheck) {
    CHECK_EQ(PROCESSING_QUEUED_DATA_POINTS, state);
  } else {
    switch (state) {
      case UNOWNED:
        return {kNotOwned, kNotOwned};
      case PRE_OWNED:
      case READING_KEYS:
        queueDataPointWithKey(key, value, category);

        // Assume the data point will be added and no new keys will be
        // added. This might not be the case but these return values
        // are only used for counters.
        return {0, 1};
      case READING_KEYS_DONE:
      case READING_LOGS:
      case PROCESSING_QUEUED_DATA_POINTS:
        if (existingItem) {
          queueDataPointWithId(id, value, category);
        } else {
          queueDataPointWithKey(key, value, category);
        }
        return {0, 1};
      case READING_BLOCK_DATA:
      case OWNED:
      case PRE_UNOWNED:
        // Continue normal processing. PRE_UNOWNED is still completely
        // considered to be owned.
        break;

        // No default case to let compiler warn if new states are added
        // without adding a case for them.
    }
  }

  if (existingItem) {
    bool added =
        putDataPointWithId(&existingItem->second, id, value, category, key);
    return {0, added ? 1 : 0};
  }

  uint32_t b = bucket(value.unixTime);

  // Prepare a row now to minimize critical section.
  auto newRow = std::make_shared<std::pair<std::string, BucketedTimeSeries>>();
  newRow->first = key;
  newRow->second.reset(n_);
  newRow->second.put(b, value, &storage_, -1, &category);

  int index = 0;
  {
    // Lock the map again.
    folly::RWSpinLock::WriteHolder writeGuard(lock_);

    // The value here doesn't matter because it will be replaced later.
    auto ret = map_.insert(std::make_pair(newRow->first.c_str(), -1));
    if (!ret.second) {
      // Nothing was inserted, just update the existing one.
      bool added = putDataPointWithId(
          &rows_[ret.first->second]->second,
          ret.first->second,
          value,
          category,
          key);
      return {0, added ? 1 : 0};
    }

    // Find a row in the vector.
    if (freeList_.size()) {
      index = freeList_.top();
      freeList_.pop();
    } else {
      tableSize_++;
      rows_.emplace_back();
      index = rows_.size() - 1;
    }

    rows_[index] = newRow;
    ret.first->second = index;
  }

  uint32_t LSN;

  // Write the new key out to disk.
  LSN = logWriter_->logData(
      shardId_, index, value.unixTime, value.value, FLAGS_log_flag_insertion);

  // New key is always added by insertion.
  // So extention of timestamp needs not be updated here.
  keyWriter_->addKey(shardId_, index, newRow->first, category);

  return {1, 1};
}

bool BucketMap::deleteDataPoint(
    const std::string& key,
    const TimeValuePair& value,
    uint16_t category,
    DeleteStatusCode& ret_status,
    bool skipStateCheck) {
  State state;
  uint32_t id;
  auto existingItem = getInternal(key, state, id);

  // State check can only skipped when processing data points from the
  // queue. Data points that come in externally during processing will
  // still be queued.
  if (skipStateCheck) {
    CHECK_EQ(PROCESSING_QUEUED_DATA_POINTS, state);
  } else {
    switch (state) {
      case UNOWNED:
        return false;
      case PRE_OWNED:
      case READING_KEYS:
        // Not support for now.
        // queueDataPointWithKey(key, value, category);

        // // Assume the data point will be added and no new keys will be
        // // added. This might not be the case but these return values
        // // are only used for counters.
        // return {0, 1};
      case READING_KEYS_DONE:
      case READING_LOGS:
      case PROCESSING_QUEUED_DATA_POINTS:
        // Not support for now.
        // if (existingItem) {
        //   queueDataPointWithId(id, value, category);
        // } else {
        //   queueDataPointWithKey(key, value, category);
        // }
        // return {0, 1};
      case READING_BLOCK_DATA:
      case OWNED:
      case PRE_UNOWNED:
        // Continue normal processing. PRE_UNOWNED is still completely
        // considered to be owned.
        break;

        // No default case to let compiler warn if new states are added
        // without adding a case for them.
    }
  }

  // The key exists, then check the timestamp.
  if (existingItem) {
    BucketedTimeSeries* curTimeSeries = &(existingItem->second);
    uint32_t b = bucket(value.unixTime);

    char* targetBlock;
    int64_t targetTimestamp = 0;
    int64_t tombstoneBitID;

    // First find the tombstoneBitID without set the lock bit.
    targetBlock = curTimeSeries->findDataPoint(
        getStorage(), b, value, tombstoneBitID, targetTimestamp);

    // The key and timestamp exists;
    if (tombstoneBitID != FLAGS_data_point_nonexistent &&
        tombstoneBitID != FLAGS_data_point_in_updatedDataMap) {
      // The database is not running for transaction, so lock the data point.
      if (!FLAGS_is_transaction) {
        existingItem->second.lockDataPoint(
            targetBlock, b, tombstoneBitID, value);
      }

      // The data point should be confirmed existence to avoid
      // other thread delete it before lock bit setted.
      if (!existingItem->second.confirmDataPointExits(
              targetBlock, b, tombstoneBitID, value, targetTimestamp)) {
        ret_status = DeleteStatusCode::VALUE_MISSING;
        return false;
      }

      // // Ohter thread has called the checkpoint,
      // // The current thread should call it as well.
      // if (FLAGS_is_checkpoint && checkpointFlag_) {
      //   checkpointing_++;
      //   checkpoint();
      // }

      // Write a log out to disk.
      uint64_t LSN = logWriter_->logData(
          shardId_,
          id,
          value.unixTime,
          value.value,
          FLAGS_log_flag_deletion,
          targetTimestamp);

      // // Whenever the LSN larger than FLAGS_max_allowed_LSN,
      // // the checkpoint should be called.
      // if (FLAGS_is_checkpoint && LSN == FLAGS_max_allowed_LSN) {
      //   checkpoint();
      //   LSN = 1;
      // }

      // Set the tombstone.
      // If the data point is in updatedDataMap, erase it at the same time.
      existingItem->second.setTombstone(
          targetBlock, b, tombstoneBitID, value, LSN, true);

      // The database is not running for transaction, so unlock the data point.
      if (!FLAGS_is_transaction) {
        existingItem->second.unlockDataPoint(
            targetBlock, b, tombstoneBitID, value);
      }

      if (existingItem->second.getCurrentActiveBucket() != b)
        getStorage()->getBuckets()[b % n_].finalized = false;
      ret_status = DeleteStatusCode::OK_ALL;
      return true;
    }
    // The data point just exists in the updatedDataMap.
    else if (tombstoneBitID == FLAGS_data_point_in_updatedDataMap) {
      // // Ohter thread has called the checkpoint,
      // // The current thread should call it as well.
      // if (FLAGS_is_checkpoint && checkpointFlag_) {
      //   checkpointing_++;
      //   checkpoint();
      // }

      targetTimestamp = existingItem->second.readTimestampFromDataMap(value);

      // Write a log out to disk.
      uint64_t LSN = logWriter_->logData(
          shardId_, id, targetTimestamp, value.value, FLAGS_log_flag_deletion);

      // // Whenever the LSN larger than FLAGS_max_allowed_LSN,
      // // the checkpoint should be called.
      // if (FLAGS_is_checkpoint && LSN == FLAGS_max_allowed_LSN) {
      //   checkpoint();
      //   LSN = 1;
      // }
      existingItem->second.deleteDataPointInDataMap(value);

      if (existingItem->second.getCurrentActiveBucket() != b)
        getStorage()->getBuckets()[b % n_].finalized = false;
      ret_status = DeleteStatusCode::OK_ALL;
      return true;
    }
    // The timestamp does not exist;
    else {
      ret_status = DeleteStatusCode::VALUE_MISSING;
      return false;
    }
  }
  // The key does not exist.
  else {
    ret_status = DeleteStatusCode::KEY_MISSING;
    return false;
  }
}

bool BucketMap::updateDataPoint(
    const std::string& key,
    const TimeValuePair& value,
    uint16_t category,
    UpdateStatusCode& ret_status,
    bool skipStateCheck) {
  State state;
  uint32_t id;
  auto existingItem = getInternal(key, state, id);

  // State check can only skipped when processing data points from the
  // queue. Data points that come in externally during processing will
  // still be queued.
  if (skipStateCheck) {
    CHECK_EQ(PROCESSING_QUEUED_DATA_POINTS, state);
  } else {
    switch (state) {
      case UNOWNED:
        // Not owning this shard, caller has stale shard information.
        ret_status = UpdateStatusCode::SHARD_IN_PROGRESS;
        return false;
      case PRE_OWNED:
      case READING_KEYS:
        // Not support for now.
        // queueDataPointWithKey(key, value, category);

        // // Assume the data point will be added and no new keys will be
        // // added. This might not be the case but these return values
        // // are only used for counters.
        // return {0, 1};
      case READING_KEYS_DONE:
      case READING_LOGS:
      case PROCESSING_QUEUED_DATA_POINTS:
        // Not support for now.
        // if (existingItem) {
        //   queueDataPointWithId(id, value, category);
        // } else {
        //   queueDataPointWithKey(key, value, category);
        // }
        // return {0, 1};

        // Not ready to serve reads yet.
        ret_status = UpdateStatusCode::SHARD_IN_PROGRESS;
        return false;
      case READING_BLOCK_DATA:
      case OWNED:
      case PRE_UNOWNED:
        // Continue normal processing. PRE_UNOWNED is still completely
        // considered to be owned.
        break;

        // No default case to let compiler warn if new states are added
        // without adding a case for them.
    }
  }

  // The key exists, then check the timestamp.
  if (existingItem) {
    BucketedTimeSeries* curTimeSeries = &(existingItem->second);
    uint32_t b = bucket(value.unixTime);

    char* targetBlock;
    int64_t targetTimestamp = 0;
    int64_t tombstoneBitID;

    // Lock current stream of existingItem.
    folly::RWSpinLock::ReadHolder readGuard(
        existingItem->second.getStreamLock());

    // First find the tombstoneBitID without set the lock bit.
    targetBlock = curTimeSeries->findDataPoint(
        getStorage(), b, value, tombstoneBitID, targetTimestamp);

    // LOG(INFO) << "The target block address is  " << targetBlock << std::endl;

    // The key and timestamp exists;
    if (tombstoneBitID != FLAGS_data_point_nonexistent &&
        tombstoneBitID != FLAGS_data_point_in_updatedDataMap) {
      // The database is not running for transaction, so lock the data point.
      if (!FLAGS_is_transaction) {
        existingItem->second.lockDataPoint(
            targetBlock, b, tombstoneBitID, value);
      }

      // The data point should be confirmed existence to avoid
      // other threads delete it before the lock bit setted.
      if (!existingItem->second.confirmDataPointExits(
              targetBlock, b, tombstoneBitID, value, targetTimestamp)) {
        ret_status = UpdateStatusCode::VALUE_MISSING;
        return false;
      }

      // // Ohter thread has called the checkpoint,
      // // The current thread should call it as well.
      // if (FLAGS_is_checkpoint && checkpointFlag_) {
      //   checkpointing_++;
      //   checkpoint();
      // }

      // Write a log out to disk.
      uint64_t LSN = logWriter_->logData(
          shardId_,
          id,
          value.unixTime,
          value.value,
          FLAGS_log_flag_update,
          targetTimestamp);

      // // Whenever the LSN larger than FLAGS_max_allowed_LSN,
      // // the checkpoint should be called.
      // if (FLAGS_is_checkpoint && LSN == FLAGS_max_allowed_LSN) {
      //   checkpoint();
      //   LSN = 1;
      // }

      // Update the data point if it is in updatedDataMap,
      // otherwise, insert the data point into updatedDataMap.
      existingItem->second.updateDataPoint(LSN, value);

      // Set the tombstone.
      // If the data point is in the updatedDataMap,
      // the function will remove the original data point.
      existingItem->second.setTombstone(targetBlock, b, tombstoneBitID, value);

      // The database is not running for transaction, so unlock the data point.
      if (!FLAGS_is_transaction) {
        existingItem->second.unlockDataPoint(
            targetBlock, b, tombstoneBitID, value);
      }

      if (existingItem->second.getCurrentActiveBucket() != b)
        getStorage()->getBuckets()[b % n_].finalized = false;
      ret_status = UpdateStatusCode::OK_ALL;
      return true;
    }
    // The data point just exists in the updatedDataMap.
    else if (tombstoneBitID == FLAGS_data_point_in_updatedDataMap) {
      // // Ohter thread has called the checkpoint,
      // // The current thread should call it as well.
      // if (FLAGS_is_checkpoint && checkpointFlag_) {
      //   checkpointing_++;
      //   checkpoint();
      // }

      targetTimestamp = existingItem->second.readTimestampFromDataMap(value);

      // Write a log out to disk.
      uint64_t LSN = logWriter_->logData(
          shardId_, id, targetTimestamp, value.value, FLAGS_log_flag_update);

      // // Whenever the LSN larger than FLAGS_max_allowed_LSN,
      // // the checkpoint should be called.
      // if (FLAGS_is_checkpoint && LSN == FLAGS_max_allowed_LSN) {
      //   checkpoint();
      //   LSN = 1;
      // }
      existingItem->second.updateDataPointInDataMap(value, LSN);

      if (existingItem->second.getCurrentActiveBucket() != b)
        getStorage()->getBuckets()[b % n_].finalized = false;
      ret_status = UpdateStatusCode::OK_ALL;
      return true;
    }
    // The timestamp dose not exist;
    else {
      LOG(INFO) << "The data point is missing??????????" << std::endl;
      LOG(INFO) << "The key of missing data point is " << key << std::endl;
      ret_status = UpdateStatusCode::VALUE_MISSING;
      return false;
    }
  }
  // The key does not exist.
  else {
    ret_status = UpdateStatusCode::KEY_MISSING;
    return false;
  }
}

// Get a shared_ptr to a TimeSeries.
BucketMap::Item BucketMap::get(const std::string& key) {
  State state;
  uint32_t id;
  return getInternal(key, state, id);
}

// Get all the TimeSeries.
void BucketMap::getEverything(std::vector<Item>& out) {
  out.reserve(tableSize_);
  folly::RWSpinLock::ReadHolder guard(lock_);
  out.insert(out.end(), rows_.begin(), rows_.end());
}

bool BucketMap::getSome(std::vector<Item>& out, int offset, int count) {
  out.reserve(count);
  folly::RWSpinLock::ReadHolder guard(lock_);
  if (offset >= rows_.size()) {
    return false;
  } else if (offset + count >= rows_.size()) {
    out.insert(out.end(), rows_.begin() + offset, rows_.end());
    return false;
  } else {
    out.insert(
        out.end(), rows_.begin() + offset, rows_.begin() + offset + count);
    return true;
  }
}

void BucketMap::erase(int index, Item item) {
  folly::RWSpinLock::WriteHolder guard(lock_);

  if (rows_[index] != item || !item) {
    // The arguments provided are no longer valid.
    GorillaStatsManager::addStatValue(kDeletionRaces);
    return;
  }

  auto it = map_.find(item->first.c_str());
  if (it != map_.end() && it->second == index) {
    // The map still points to the right entry.
    map_.erase(it);
  } else {
    GorillaStatsManager::addStatValue(kDeletionRaces);
  }

  rows_[index].reset();
  freeList_.push(index);
}

uint32_t BucketMap::bucket(uint64_t unixTime) const {
  return BucketUtils::bucket(unixTime, windowSize_, shardId_);
}

uint64_t BucketMap::timestamp(uint32_t bucket) const {
  return BucketUtils::timestamp(bucket, windowSize_, shardId_);
}

uint64_t BucketMap::duration(uint32_t buckets) const {
  return BucketUtils::duration(buckets, windowSize_);
}

uint32_t BucketMap::buckets(uint64_t duration) const {
  return BucketUtils::buckets(duration, windowSize_);
}

BucketStorage* BucketMap::getStorage() {
  return &storage_;
}

bool BucketMap::setState(BucketMap::State state) {
  Timer timer(true);

  // If we have to drop a shard, move the data here, then free all the memory
  // outside of any locks, as this can take a long time.
  std::unordered_map<const char*, int, CaseHash, CaseEq> tmpMap;
  std::priority_queue<int, std::vector<int>, std::less<int>> tmpQueue;
  std::vector<Item> tmpVec;
  std::vector<std::vector<uint32_t>> tmpDeviations;

  std::unique_lock<std::mutex> stateGuard(stateChangeMutex_);
  folly::RWSpinLock::WriteHolder guard(lock_);
  if (!isAllowedStateTransition(state_, state)) {
    LOG(WARNING) << "Illegal transition from " << state_ << " to " << state;
    return false;
  }

  if (state == PRE_OWNED) {
    addTimer_.start();
    keyWriter_->startShard(shardId_);
    logWriter_->startShard(shardId_);
    dataPointQueue_ = std::make_shared<folly::MPMCQueue<QueuedDataPoint>>(
        FLAGS_data_point_queue_size);

    // Deviations are indexed per minute.
    deviations_.resize(duration(n_) / kGorillaSecondsPerMinute);
  } else if (state == UNOWNED) {
    tmpMap.swap(map_);
    tmpQueue.swap(freeList_);
    tmpVec.swap(rows_);
    tmpDeviations.swap(deviations_);
    tableSize_ = 0;

    // These operations do block, but only to enqueue flags, not drain the
    // queues to disk.
    keyWriter_->stopShard(shardId_);
    logWriter_->stopShard(shardId_);
  } else if (state == OWNED) {
    // Calling this won't hurt even if the timer isn't running.
    addTimer_.stop();
  }

  BucketMap::State oldState = state_;
  state_ = state;
  guard.reset();

  // Enable/disable storage outside the lock because it might take a
  // while and the the storage object has its own locking.
  if (state == PRE_OWNED) {
    storage_.enable();
  } else if (state == UNOWNED) {
    storage_.clearAndDisable();
  }

  LOG(INFO) << "Changed state of shard " << shardId_ << " from " << oldState
            << " to " << state << " in " << timer.get() << "us";

  return true;
}

BucketMap::State BucketMap::getState() {
  folly::RWSpinLock::ReadHolder guard(lock_);
  return state_;
}

Timer::TimeVal BucketMap::getAddTime() {
  return addTimer_.get() / kGorillaUsecPerMs;
}

bool BucketMap::cancelUnowning() {
  folly::RWSpinLock::WriteHolder guard(lock_);
  if (state_ != PRE_UNOWNED) {
    return false;
  }

  state_ = OWNED;
  return true;
}

bool BucketMap::isAllowedStateTransition(State from, State to) {
  return to > from || (from == OWNED && to == PRE_UNOWNED);
}

void BucketMap::finalizeBuckets() {
  if (getState() != OWNED) {
    return;
  }

  std::vector<BucketMap::Item> timeSeriesData;
  getEverything(timeSeriesData);
  for (int i = 0; i < timeSeriesData.size(); i++) {
    if (timeSeriesData[i].get()) {
      timeSeriesData[i]->second.setCurrentBucket(
          getStorage()->getNewestPosition() - n_ + 1,
          getStorage(),
          i); // `i` is the id of the time series
    }
  }
  getStorage()->finalizeBuckets();
}

void BucketMap::finalizeStreams(std::vector<TimeSeriesStream>& streams) {
  FileUtils::File f =
      getStorage()->getDataFileUtils().open(-1, "wb", kLargeFileBuffer);
  for (int i = 0; i < streams.size(); i++) {
    streams[i].finalize(i, f.file);
  }
  fflush(f.file);
  fsync(f.file->_fileno);
  fclose(f.file);
}

void BucketMap::readStreams() {
  if (!FLAGS_is_checkpoint) {
    return;
  }

  FileUtils::File f =
      getStorage()->getDataFileUtils().open(-1, "rb", 0); 
  std::vector<std::unique_ptr<DataBlock>> pointers;

  if (!f.file) {
    LOG(ERROR) << "Could not open block file for reading : " << f.name;
    return;
  }

  fseek(f.file, 0, SEEK_END);
  size_t len = ftell(f.file);
  if (len == 0) {
    LOG(WARNING) << "Empty data file " << f.name;
    fclose(f.file);
    return;
  }

  fseek(f.file, 0, SEEK_SET);
  std::unique_ptr<char[]> buffer(new char[len]);
  int bytesRead = fread(buffer.get(), sizeof(char), len, f.file);

  if (bytesRead != len) {
    PLOG(ERROR) << "Could not read metadata from " << f.name;
    fclose(f.file);
    return;
  }
  fclose(f.file);

  const char* ptr = (const char*)buffer.get();

  size_t readLen = 0;

  while (readLen < len) {
    uint32_t timeseriesId;
    memcpy(&timeseriesId, ptr, sizeof(timeseriesId));
    ptr += sizeof(timeseriesId);
    readLen += sizeof(timeseriesId);

    LOG(INFO) << "Read length is " << readLen << ". Total len is " << len;

    // Read stream from ptr.
    auto timeSeries = &(rows_[timeseriesId]->second);
    auto curReadLen = timeSeries->getStream().deserializeStream(ptr);
    ptr += curReadLen;
    readLen += curReadLen;
  }
}

bool BucketMap::isBehind(uint32_t bucketToFinalize) const {
  return lastFinalizedBucket_ != 0 &&
      bucketToFinalize > lastFinalizedBucket_ + 1;
}

void BucketMap::shutdown() {
  if (getState() == OWNED) {
    logWriter_->stopShard(shardId_);
    keyWriter_->stopShard(shardId_);

    // Set the state directly without calling setState which would try
    // to deallocate memory.
    std::unique_lock<std::mutex> stateGuard(stateChangeMutex_);
    folly::RWSpinLock::WriteHolder guard(lock_);
    state_ = UNOWNED;
  }
}

void BucketMap::compactKeyList() {
  std::vector<Item> items;
  getEverything(items);

  uint32_t i = -1;
  keyWriter_->compact(shardId_, [&]() {
    for (i++; i < items.size(); i++) {
      if (items[i].get()) {
        return std::make_tuple(
            i, items[i]->first.c_str(), items[i]->second.getCategory());
      }
    }
    return std::make_tuple<uint32_t, const char*, uint16_t>(0, nullptr, 0);
  });
}

void BucketMap::deleteOldBlockFiles() {
  // Start far enough back that we can't possibly interfere with anything.
  storage_.deleteBucketsOlderThan(bucket(time(nullptr)) - n_ - 1);
}

void BucketMap::startMonitoring() {
  GorillaStatsManager::addStatExportType(kMsPerKeyListRead, AVG);
  GorillaStatsManager::addStatExportType(kMsPerLogFilesRead, AVG);
  GorillaStatsManager::addStatExportType(kMsPerBlockFileRead, AVG);
  GorillaStatsManager::addStatExportType(kMsPerBlockFileRead, COUNT);
  GorillaStatsManager::addStatExportType(kMsPerQueueProcessing, AVG);
  GorillaStatsManager::addStatExportType(kDataPointQueueDropped, SUM);
  GorillaStatsManager::addStatExportType(kCorruptLogFiles, SUM);
  GorillaStatsManager::addStatExportType(kCorruptKeyFiles, SUM);
  GorillaStatsManager::addStatExportType(kUnknownKeysInLogFiles, SUM);
  GorillaStatsManager::addStatExportType(kUnknownKeysInBlockMetadataFiles, SUM);
  GorillaStatsManager::addStatExportType(kDataHoles, SUM);
  GorillaStatsManager::addStatExportType(kMissingLogs, SUM);
  GorillaStatsManager::addStatExportType(kMissingLogs, AVG);
  GorillaStatsManager::addStatExportType(kMissingLogs, COUNT);
  GorillaStatsManager::addStatExportType(kDeletionRaces, SUM);
  GorillaStatsManager::addStatExportType(kDuplicateKeys, SUM);
}

BucketMap::Item
BucketMap::getInternal(const std::string& key, State& state, uint32_t& id) {
  folly::RWSpinLock::ReadHolder guard(lock_);

  state = state_;
  if (state_ >= UNOWNED && state_ <= READING_KEYS) {
    // Either the state is UNOWNED or keys are being read. In both
    // cases do not try to find the key.
    return nullptr;
  }

  const auto& it = map_.find(key.c_str());
  if (it != map_.end()) {
    id = it->second;
    return rows_[id];
  }

  return nullptr;
}

void BucketMap::findBlocks() {
  DataBlockReader reader(shardId_, dataDirectory_);
  {
    std::unique_lock<std::mutex> guard(unreadBlockFilesMutex_);
    unreadBlockFiles_ = reader.findCompletedBlockFiles();
    if (unreadBlockFiles_.size() > 0) {
      checkForMissingBlockFiles();
      // lastFinalizedBucket_ = *unreadBlockFiles_.rbegin();
    }
  }
}

void BucketMap::readData() {
  // bool success = setState(READING_LOGS);
  // CHECK(success) << "Setting state failed";

  Timer timer(true);

  // DataBlockReader reader(shardId_, dataDirectory_);
  // {
  //   std::unique_lock<std::mutex> guard(unreadBlockFilesMutex_);
  //   unreadBlockFiles_ = reader.findCompletedBlockFiles();
  //   if (unreadBlockFiles_.size() > 0) {
  //     checkForMissingBlockFiles();
  //     // lastFinalizedBucket_ = *unreadBlockFiles_.rbegin();
  //   }
  // }

  // readLogFiles(lastFinalizedBucket_);

  readLogFilesParallel(lastFinalizedBucket_);

  GorillaStatsManager::addStatValue(
      kMsPerLogFilesRead, timer.reset() / kGorillaUsecPerMs);
  CHECK(getState() == READING_LOGS);

  bool success = setState(PROCESSING_QUEUED_DATA_POINTS);
  CHECK(success);

  // Skip state check when processing queued data points.
  processQueuedDataPoints(true);

  // There's a tiny chance that incoming data points will think that
  // the state is PROCESSING_QUEUED_DATA_POINTS and they will be
  // queued after the second call to processQueuedDataPoints.
  success = setState(READING_BLOCK_DATA);
  CHECK(success);

  // Process queued data points again, just to be sure that the queue
  // is empty because it is possible that something was inserted into
  // the queue after it was emptied and before the state was set to
  // READING_BLOCK_DATA.
  processQueuedDataPoints(false);
  GorillaStatsManager::addStatValue(
      kMsPerQueueProcessing, timer.reset() / kGorillaUsecPerMs);

  // Take a copy of the shared pointer to avoid freeing the memory
  // while holding the write lock. Not the most elegant solution but it
  // guarantees that freeing memory won't block anything else.
  std::shared_ptr<folly::MPMCQueue<QueuedDataPoint>> copy;
  {
    folly::RWSpinLock::WriteHolder guard(lock_);
    copy = dataPointQueue_;
    dataPointQueue_.reset();
  }

  // Probably not needed because this object will fall out of scope,
  // but I am afraid of compiler optimizations that might end up
  // freeing the memory inside the write lock.
  copy.reset();

  success = setState(OWNED);
  CHECK(success);
}

bool BucketMap::readBlockFiles() {
  if (!FLAGS_is_checkpoint) {
  // bool success = setState(OWNED);
    bool success = setState(READING_LOGS);
    CHECK(success);
    // Done reading block files.
    return false;
  }

  int32_t position;
  {
    std::unique_lock<std::mutex> guard(unreadBlockFilesMutex_);
    if (unreadBlockFiles_.empty()) {
      // bool success = setState(OWNED);
      bool success = setState(READING_LOGS);
      CHECK(success);
      // Done reading block files.
      return false;
    }

    position = *unreadBlockFiles_.rbegin();
    // if (position != -1) {
    unreadBlockFiles_.erase(position);
    // }
  }

  std::vector<uint32_t> timeSeriesIds;
  std::vector<uint64_t> storageIds;

  LOG(INFO) << "Reading blockfiles for shard " << shardId_ << ": " << position;
  Timer timer(true);
  if (storage_.loadPosition(position, timeSeriesIds, storageIds)) {
    folly::RWSpinLock::ReadHolder guard(lock_);

    for (int i = 0; i < timeSeriesIds.size(); i++) {
      if (timeSeriesIds[i] < rows_.size() && rows_[timeSeriesIds[i]].get()) {
        rows_[timeSeriesIds[i]]->second.setDataBlock(
            position, n_, storageIds[i]);
      } else {
        GorillaStatsManager::addStatValue(kUnknownKeysInBlockMetadataFiles);
      }
    }

    GorillaStatsManager::addStatValue(
        kMsPerBlockFileRead, timer.reset() / kGorillaUsecPerMs);
    LOG(INFO) << "Done reading blockfiles for shard " << shardId_ << ": "
              << position;
  } else {
    // This could just be because we've read the data before, but that shouldn't
    // happen (it gets cleared on shard drop). Bump the counter anyway.
    LOG(ERROR) << "Failed to read blockfiles for shard " << shardId_ << ": "
               << position << ". Already loaded?";
  }

  return true;
}

void BucketMap::readKeyList() {
  LOG(INFO) << "Reading keys for shard " << shardId_;
  Timer timer(true);

  bool success = setState(READING_KEYS);
  CHECK(success) << "Setting state failed";

  // No reason to lock because nothing is touching the rows_ or map_
  // while this is running.

  // Read all the keys from disk into the vector.
  PersistentKeyList::readKeys(
      shardId_,
      *(keyWriter_->getDataDirectory()),
      [&](uint32_t id, const char* key, uint16_t category) {
        if (strlen(key) >= kMaxAllowedKeyLength) {
          LOG(ERROR) << "Key too long. Key file is corrupt for shard "
                     << shardId_;
          GorillaStatsManager::addStatValue(kCorruptKeyFiles);

          // Don't continue reading from this file anymore.
          return false;
        }

        if (id > FLAGS_max_allowed_timeseries_id) {
          LOG(ERROR) << "ID is too large. Key file is corrupt for shard "
                     << shardId_;
          GorillaStatsManager::addStatValue(kCorruptKeyFiles);

          // Don't continue reading from this file anymore.
          return false;
        }

        if (id >= rows_.size()) {
          rows_.resize(id + kRowsAtATime);
        }

        rows_[id].reset(new std::pair<std::string, BucketedTimeSeries>());
        rows_[id]->first = key;
        rows_[id]->second.reset(n_);
        rows_[id]->second.setCategory(category);
        return true;
      });

  tableSize_ = rows_.size();
  map_.reserve(rows_.size());

  // Put all the rows in either the map or the free list.
  for (int i = 0; i < rows_.size(); i++) {
    if (rows_[i].get()) {
      auto result = map_.insert({rows_[i]->first.c_str(), i});

      // Ignore keys that already exist.
      if (!result.second) {
        GorillaStatsManager::addStatValue(kDuplicateKeys);
        rows_[i].reset();
        freeList_.push(i);
      }
    } else {
      freeList_.push(i);
    }
  }

  LOG(INFO) << "Done reading keys for shard " << shardId_;
  GorillaStatsManager::addStatValue(
      kMsPerKeyListRead, timer.reset() / kGorillaUsecPerMs);
  success = setState(READING_KEYS_DONE);
  CHECK(success) << "Setting state failed";
}

void BucketMap::readLogFilesParallel(uint32_t lastBlock) {
  LOG(INFO) << "Reading logs for shard " << shardId_;
  FileUtils files(
      shardId_,
      BucketLogWriter::kLogFilePrefix,
      *(logWriter_->getDataDirectory()));

  // const bool& stopInsertThread = stopInsertThreads[shardId_];
  // const bool& isInsertThreadRunning = isInsertThreadRunnings[shardId_];

  // Timestamp of the last finished checkpoint.
  int64_t lastCheckpoint = 0;

  FileUtils checkpointFiles(
      shardId_,
      BucketLogWriter::kCheckpointPrefix,
      *(logWriter_->getDataDirectory()));

  std::vector<int64_t> ids = files.ls();

  // Record the offsets of each log file in the last checkpoint.
  std::vector<int64_t> offsets(ids.size(), 0);

  // Record the previous timestamps of each log file in the last checkpoint.
  std::vector<int64_t> preTimestamps(ids.size(), 0);
  
  if(FLAGS_is_checkpoint) {
    std::vector<boost::filesystem::path> checkPointPaths;
    checkPointPaths.reserve(checkpointFiles.ls().size());

    for(auto checkPointID : checkpointFiles.ls()){
      checkPointPaths.emplace_back(checkpointFiles.getPath(checkPointID));
    }
    
    lastCheckpoint = DataLogReader::readCheckpoint(checkPointPaths, offsets, preTimestamps);
  }

  

  uint32_t unknownKeys = 0;
  int64_t lastTimestamp = timestamp(lastBlock + 1);

  // Record the logs with insertion flag for each log file.
  std::vector<std::shared_ptr<folly::MPMCQueue<RecoveryLogDataInfo>>>
      insertLogs(FLAGS_threads_for_recovery);

  // Record the logs with update flags for each log file.
  // Each MPMCQueue records logs with the same LSN.
  // So the LSN of logs in updateLogs is from 1 to FLAGS_max_allowed_LSN.
  std::vector<std::shared_ptr<folly::MPMCQueue<RecoveryLogDataInfo>>>
      updateLogs(FLAGS_max_allowed_LSN + 1);

  // std::vector<std::shared_ptr<std::vector<RecoveryLogDataInfo>>>
  //     updateLogs(FLAGS_max_allowed_LSN + 1);

  // Initialize threads to perform data insertion for recovery.

  std::mutex insertThreadMutex;

  std::condition_variable insertThreadCon;

  std::vector<std::thread> threads;

  if (ids.size() != 0 && ids.back() > lastCheckpoint) {
    // Initialize the size of MPMCQueue.
    for (int i = 0; i < FLAGS_threads_for_recovery; ++i) {
      insertLogs[i].reset(
          new folly::MPMCQueue<RecoveryLogDataInfo>(FLAGS_logs_queue_size));
    }

    // Initialize the size of MPMCQueue.
    for (int i = 0; i < FLAGS_max_allowed_LSN + 1; ++i) {
      // For each workload, there maybe as much as 4 million update logs.
      // And the number of update logs is approximately inversely proportional
      // to LSN.
      int queueSize;
      if (i < 3) {
        queueSize = FLAGS_logs_queue_size * (FLAGS_max_allowed_LSN - i + 1) * 4;
      } else {
        queueSize =
            FLAGS_logs_queue_size * (FLAGS_max_allowed_LSN - i + 1) / 10;
      }
      updateLogs[i].reset(new folly::MPMCQueue<RecoveryLogDataInfo>(queueSize));
      // updateLogs[i].reset(
      //     new std::vector<RecoveryLogDataInfo>());
    }

    for (int i = 0; i < FLAGS_threads_for_recovery; ++i) {
      threads.emplace_back([&, i]() {
        RecoveryLogDataInfo logInfo;

        // Running until isInsertThreadRunning is false.
        while (!stopInsertThreads[shardId_]) {
          std::unique_lock<std::mutex> conGuard(insertThreadMutex);

          // Wait until the other thread have flushed the log with LSN > 0.
          if (!isInsertThreadRunnings[shardId_]) {
            insertThreadCon.wait(conGuard);
          }

          while (insertLogs[i]->read(logInfo)) {
            rows_[logInfo.index]->second.put(
                bucket(logInfo.tv.unixTime),
                logInfo.tv,
                &storage_,
                logInfo.index,
                nullptr);
          }
        }
      });
    }
  }

  int numOfPoints = 0;
  // Record the the thread ID to read the Log file.
  uint32_t curFile = 0;
  
  // Redo each data log file.
  for (int i = 0; i < ids.size(); ) {
    // The logs with ids before the last finished checkpoint should not be redone.
    // if (ids[i] / 10 < lastCheckpoint && FLAGS_is_checkpoint) {
    //   for (int j = 0; j < FLAGS_number_of_logging_queues; ++j) {
    //     LOG(INFO) << "Skipping log file " << ids[i+j] << " because it's already "
    //                 << "covered by a checkpoint";
    //   }
    //   i += FLAGS_number_of_logging_queues;
    //   continue;
    // }

    int64_t id = ids[i];
    // Used for analyze logs when using logging pipeline
    std::vector<boost::filesystem::path> paths(FLAGS_number_of_logging_queues);

    // The length of log file names is 11.
    // The last number indicates the logging pipeline thread ID.
    // int64_t baseTime = id / 10;
    std::vector<int64_t> baseTimes(FLAGS_number_of_logging_queues);

    std::vector<int64_t> curOffsets(FLAGS_number_of_logging_queues);

    std::vector<int64_t> curPreTimestamps(FLAGS_number_of_logging_queues);

    isInsertThreadRunnings[shardId_] = true;
    insertThreadCon.notify_all();

    if (FLAGS_using_libpmem_or_libpmem2) {
      for (int j = 0; j < FLAGS_number_of_logging_queues; ++j) {
        paths[j] = files.getPath(ids[i + j]);
        baseTimes[j] = ids[i + j] / 10;
        curOffsets[j] = offsets[i + j];
        curPreTimestamps[j] = preTimestamps[i + j] == 0 ? baseTimes[j] : preTimestamps[i + j];
        // if (baseTimes[j] < timestamp(lastBlock + 1) && FLAGS_is_checkpoint) {
        //   LOG(INFO) << "Skipping log file " << ids[i + j] << " because it's already "
        //             << "covered by a block";
        //   continue;
        // }
      }
      i += FLAGS_number_of_logging_queues;

      uint32_t b = bucket(baseTimes[0]);

      // Read logs and push them into insertLogs or updateLogs.
      numOfPoints = DataLogReader::readLog(
          paths,
          baseTimes,
          curOffsets,
          curPreTimestamps,
          curFile,
          [&](const uint32_t& key,
              const int64_t& unixTime,
              const double& value,
              const int16_t& flag,
              const uint32_t& LSN) {
            if (LSN == 0 && (unixTime < timestamp(b) || unixTime > timestamp(b + 1))) {
              LOG(ERROR) << "Unix time is out of the expected range: "
                         << unixTime << " [" << timestamp(b) << ","
                         << timestamp(b + 1) << "]";
              GorillaStatsManager::addStatValue(kCorruptLogFiles);

              // It's better to stop reading this log file here because
              // none of the data can be trusted after this.
              return false;
            }

            folly::RWSpinLock::ReadHolder guard(lock_);

            if (key < rows_.size() && rows_[key].get()) {
              RecoveryLogDataInfo info;
              info.flag = flag;
              info.index = key;
              info.tv.unixTime = unixTime;
              info.tv.value = value;
              uint32_t pos;
              switch (flag) {
                case log_insertion:
                  pos = key % FLAGS_threads_for_recovery;
                  insertLogs[pos]->blockingWrite(std::move(info));
                  // LOG(INFO) << "Insert a log, the key is: usermetric" <<
                  // info.index << std::endl;
                  break;

                case log_deletion:
                  // LOG(ERROR) << "The LSN is: " << LSN;
                  updateLogs[LSN]->blockingWrite(std::move(info));
                  // updateLogs[LSN]->emplace_back(std::move(info));
                  break;

                case log_update:
                  // LOG(ERROR) << "The LSN is: " << LSN;
                  updateLogs[LSN]->blockingWrite(std::move(info));
                  // updateLogs[LSN]->emplace_back(std::move(info));
                  break;

                default:
                  LOG(ERROR) << "Invalid log flag " << flag;
              }
            } else {
              unknownKeys++;
              LOG(INFO) << "UnknowsKeys: " << key << std::endl;
            }

            int64_t gap = unixTime - lastTimestamp;
            if (gap > FLAGS_missing_logs_threshold_secs &&
                lastTimestamp > timestamp(1)) {
              LOG(ERROR) << gap << " seconds of missing logs from "
                         << lastTimestamp << " to " << unixTime << " for shard "
                         << shardId_;
              GorillaStatsManager::addStatValue(kDataHoles, 1);
              GorillaStatsManager::addStatValue(kMissingLogs, gap);
              reliableDataStartTime_ = unixTime;
            }
            lastTimestamp = std::max(lastTimestamp, unixTime);

            return true;
          });

      LOG(INFO) << "The number of points in file " << id << " is "
                << numOfPoints;

      // for (int i = 0; i < FLAGS_threads_for_recovery; ++i) {
      //   // Restart threads for the next log file.
      //   threads[i] = std::thread([&, i]() {
      //     RecoveryLogDataInfo logInfo;
      //     // Running until isInsertThreadRunning is false.
      //     while(!stopInsertThread) {
      //       std::unique_lock<std::mutex> conGuard(insertThreadMutex);

      //       // Wait until the other thread have flushed the log with LSN > 0.
      //       if (!isInsertThreadRunning){
      //         insertThreadCon.wait(conGuard);
      //       }

      //       while(insertLogs[i]->read(logInfo)){
      //         rows_[logInfo.index]->second.put(
      //               bucket(logInfo.tv.unixTime), logInfo.tv, &storage_,
      //               logInfo.index, nullptr);
      //       }
      //     }
      //   });
      // }
    } else {
      auto file = files.open(id, "rb", 0);
      if (!file.file) {
        LOG(ERROR) << "Could not open logfile for reading";
        continue;
      }

      uint32_t b = bucket(baseTimes[0]);
      DataLogReader::readLog(
          file,
          id,
          [&](uint32_t key, int64_t unixTime, double value, int16_t, uint32_t) {
            if (unixTime < timestamp(b) || unixTime > timestamp(b + 1)) {
              LOG(ERROR) << "Unix time is out of the expected range: "
                         << unixTime << " [" << timestamp(b) << ","
                         << timestamp(b + 1) << "]";
              GorillaStatsManager::addStatValue(kCorruptLogFiles);

              // It's better to stop reading this log file here because
              // none of the data can be trusted after this.
              return false;
            }

            folly::RWSpinLock::ReadHolder guard(lock_);
            if (key < rows_.size() && rows_[key].get()) {
              TimeValuePair tv;
              tv.unixTime = unixTime;
              tv.value = value;
              rows_[key]->second.put(
                  bucket(unixTime), tv, &storage_, key, nullptr);
            } else {
              unknownKeys++;
            }

            int64_t gap = unixTime - lastTimestamp;
            if (gap > FLAGS_missing_logs_threshold_secs &&
                lastTimestamp > timestamp(1)) {
              LOG(ERROR) << gap << " seconds of missing logs from "
                         << lastTimestamp << " to " << unixTime << " for shard "
                         << shardId_;
              GorillaStatsManager::addStatValue(kDataHoles, 1);
              GorillaStatsManager::addStatValue(kMissingLogs, gap);
              reliableDataStartTime_ = unixTime;
            }
            lastTimestamp = std::max(lastTimestamp, unixTime);

            return true;
          });
      fclose(file.file);
    }
    paths.clear();
  }
  // Wait for insertion finished.
  // And reset the insertion threads.
  stopInsertThreads[shardId_] = true;
  isInsertThreadRunnings[shardId_] = true;
  insertThreadCon.notify_all();
  if (ids.size() != 0) {
    for (int i = 0; i < FLAGS_threads_for_recovery; ++i) {
      // Wait for insertion finished.
      threads[i].join();
    }

    stopInsertThreads[shardId_] = false;
    isInsertThreadRunnings[shardId_] = false;

    // Recover deletion and update by LSN.
    for (int i = 1; i < FLAGS_max_allowed_LSN + 1; ++i) {
      auto elem = updateLogs[i];
      RecoveryLogDataInfo logInfo;
      // Read out all the logs with the same LSN.
      while (elem->read(logInfo)) {
        // for(auto logInfo : *elem) {
        // Redo update.
        if (logInfo.flag == FLAGS_log_flag_update) {
          BucketedTimeSeries* curTimeSeries = &(rows_[logInfo.index]->second);
          uint32_t b = bucket(logInfo.tv.unixTime);

          char* targetBlock;
          int64_t targetTimestamp = 0;
          int64_t tombstoneBitID;

          // First find the tombstoneBitID without set the lock bit.
          targetBlock = curTimeSeries->findDataPoint(
              getStorage(), b, logInfo.tv, tombstoneBitID, targetTimestamp);

          if (tombstoneBitID > 0) {
            // Update the data point if it is in updatedDataMap,
            // otherwise, insert the data point into updatedDataMap.
            curTimeSeries->updateDataPoint(0, logInfo.tv);

            curTimeSeries->setTombstone(
                targetBlock, b, tombstoneBitID, logInfo.tv);
          } else {
            LOG(INFO) << "The data point does not exist! ID: " << logInfo.index
                      << " Key: " << rows_[logInfo.index]->first.c_str()
                      << std::endl;
          }

        }
        // Redo deletion.
        else {
          BucketedTimeSeries* curTimeSeries = &(rows_[logInfo.index]->second);
          uint32_t b = bucket(logInfo.tv.unixTime);

          char* targetBlock;
          int64_t targetTimestamp = 0;
          int64_t tombstoneBitID;

          // First find the tombstoneBitID without set the lock bit.
          targetBlock = curTimeSeries->findDataPoint(
              getStorage(), b, logInfo.tv, tombstoneBitID, targetTimestamp);
        }
      }
    }
  }

  int64_t now = time(nullptr);
  int64_t gap = now - lastTimestamp;
  if (gap > FLAGS_missing_logs_threshold_secs && lastTimestamp > timestamp(1)) {
    LOG(ERROR) << gap << " seconds of missing logs from " << lastTimestamp
               << " to now (" << now << ") for shard " << shardId_;
    GorillaStatsManager::addStatValue(kDataHoles, 1);
    GorillaStatsManager::addStatValue(kMissingLogs, gap);
    reliableDataStartTime_ = now;
  }

  LOG(INFO) << "Done reading logs for shard " << shardId_;
  LOG(INFO) << unknownKeys << " unknown keys found";
  GorillaStatsManager::addStatValue(kUnknownKeysInLogFiles, unknownKeys);
}

void BucketMap::readLogFiles(uint32_t lastBlock) {
  LOG(INFO) << "Reading logs for shard " << shardId_;
  FileUtils files(
      shardId_,
      BucketLogWriter::kLogFilePrefix,
      *(logWriter_->getDataDirectory()));

  uint32_t unknownKeys = 0;
  int64_t lastTimestamp = timestamp(lastBlock + 1);
  uint32_t curFile = 0;

  std::vector<boost::filesystem::path> paths(1);

  for (int64_t id : files.ls()) {
    if (id < timestamp(lastBlock + 1)) {
      LOG(INFO) << "Skipping log file " << id << " because it's already "
                << "covered by a block";
      continue;
    }

    // The length of log file names is 11.
    // The last number indicates the logging pipeline thread ID.
    std::vector<int64_t> baseTimes(FLAGS_number_of_logging_queues, id);

    std::vector<int64_t> curOffsets(FLAGS_number_of_logging_queues, 0);

    std::vector<int64_t> curPreTimestamps(FLAGS_number_of_logging_queues, 0);

    if (FLAGS_using_libpmem_or_libpmem2) {
      paths[0] = files.getPath(id);
      // path += "-0";
      uint32_t b = bucket(id);

      DataLogReader::readLog(
          paths,
          baseTimes,
          curOffsets,
          curPreTimestamps,
          curFile,
          [&](uint32_t key,
              int64_t unixTime,
              double value,
              int16_t flag,
              uint32_t LSN) {
            if (unixTime < timestamp(b) || unixTime > timestamp(b + 1)) {
              LOG(ERROR) << "Unix time is out of the expected range: "
                         << unixTime << " [" << timestamp(b) << ","
                         << timestamp(b + 1) << "]";
              GorillaStatsManager::addStatValue(kCorruptLogFiles);

              // It's better to stop reading this log file here because
              // none of the data can be trusted after this.
              return false;
            }

            folly::RWSpinLock::ReadHolder guard(lock_);

            switch (flag) {
              case log_insertion:
                if (key < rows_.size() && rows_[key].get()) {
                  TimeValuePair tv;
                  tv.unixTime = unixTime;
                  tv.value = value;
                  rows_[key]->second.put(
                      bucket(unixTime), tv, &storage_, key, nullptr);
                } else {
                  unknownKeys++;
                }
                break;
              case log_deletion:
                // call the data point deletion procedure
                // if (key < rows_.size() && rows_[key].get()) {
                //   TimeValuePair tv;
                //   tv.unixTime = unixTime;
                //   tv.value = value;
                //   rows_[key]->second.findDataPoint(getStorage(), b, tv.value,
                //   ) rows_[key]->second.delete(
                //       bucket(unixTime), tv, &storage_, key, nullptr, LSN);
                // } else {
                //   unknownKeys++;
                // }
                // break;
              case log_update:
                // call the update procedure

                // if (key < rows_.size() && rows_[key].get()) {
                //   TimeValuePair tv;
                //   tv.unixTime = unixTime;
                //   tv.value = value;
                //   rows_[key]->second.update(
                //       bucket(unixTime), tv, &storage_, key, nullptr, LSN);
                // } else {
                //   unknownKeys++;
                // }
                break;
              default:
                LOG(ERROR) << "Invalid log flag " << flag;
            }

            int64_t gap = unixTime - lastTimestamp;
            if (gap > FLAGS_missing_logs_threshold_secs &&
                lastTimestamp > timestamp(1)) {
              LOG(ERROR) << gap << " seconds of missing logs from "
                         << lastTimestamp << " to " << unixTime << " for shard "
                         << shardId_;
              GorillaStatsManager::addStatValue(kDataHoles, 1);
              GorillaStatsManager::addStatValue(kMissingLogs, gap);
              reliableDataStartTime_ = unixTime;
            }
            lastTimestamp = std::max(lastTimestamp, unixTime);

            return true;
          });
    } else {
      auto file = files.open(id, "rb", 0);
      if (!file.file) {
        LOG(ERROR) << "Could not open logfile for reading";
        continue;
      }

      uint32_t b = bucket(id);
      DataLogReader::readLog(
          file,
          id,
          [&](uint32_t key, int64_t unixTime, double value, int16_t, uint32_t) {
            if (unixTime < timestamp(b) || unixTime > timestamp(b + 1)) {
              LOG(ERROR) << "Unix time is out of the expected range: "
                         << unixTime << " [" << timestamp(b) << ","
                         << timestamp(b + 1) << "]";
              GorillaStatsManager::addStatValue(kCorruptLogFiles);

              // It's better to stop reading this log file here because
              // none of the data can be trusted after this.
              return false;
            }

            folly::RWSpinLock::ReadHolder guard(lock_);
            if (key < rows_.size() && rows_[key].get()) {
              TimeValuePair tv;
              tv.unixTime = unixTime;
              tv.value = value;
              rows_[key]->second.put(
                  bucket(unixTime), tv, &storage_, key, nullptr);
            } else {
              unknownKeys++;
            }

            int64_t gap = unixTime - lastTimestamp;
            if (gap > FLAGS_missing_logs_threshold_secs &&
                lastTimestamp > timestamp(1)) {
              LOG(ERROR) << gap << " seconds of missing logs from "
                         << lastTimestamp << " to " << unixTime << " for shard "
                         << shardId_;
              GorillaStatsManager::addStatValue(kDataHoles, 1);
              GorillaStatsManager::addStatValue(kMissingLogs, gap);
              reliableDataStartTime_ = unixTime;
            }
            lastTimestamp = std::max(lastTimestamp, unixTime);

            return true;
          });
      fclose(file.file);
    }
  }

  int64_t now = time(nullptr);
  int64_t gap = now - lastTimestamp;
  if (gap > FLAGS_missing_logs_threshold_secs && lastTimestamp > timestamp(1)) {
    LOG(ERROR) << gap << " seconds of missing logs from " << lastTimestamp
               << " to now (" << now << ") for shard " << shardId_;
    GorillaStatsManager::addStatValue(kDataHoles, 1);
    GorillaStatsManager::addStatValue(kMissingLogs, gap);
    reliableDataStartTime_ = now;
  }

  LOG(INFO) << "Done reading logs for shard " << shardId_;
  LOG(INFO) << unknownKeys << " unknown keys found";
  GorillaStatsManager::addStatValue(kUnknownKeysInLogFiles, unknownKeys);
}

void BucketMap::queueDataPointWithKey(
    const std::string& key,
    const TimeValuePair& value,
    uint16_t category) {
  if (key == "") {
    LOG(WARNING) << "Not queueing with empty key";
    return;
  }

  QueuedDataPoint dp;
  dp.key = key;
  dp.unixTime = value.unixTime;
  dp.value = value.value;
  dp.category = category;

  queueDataPoint(dp);
}

void BucketMap::queueDataPointWithId(
    uint32_t id,
    const TimeValuePair& value,
    uint16_t category) {
  QueuedDataPoint dp;

  // Leave key string empty to indicate that timeSeriesId is used.
  dp.timeSeriesId = id;
  dp.unixTime = value.unixTime;
  dp.value = value.value;
  dp.category = category;

  queueDataPoint(dp);
}

void BucketMap::queueDataPoint(QueuedDataPoint& dp) {
  std::shared_ptr<folly::MPMCQueue<QueuedDataPoint>> queue;
  {
    folly::RWSpinLock::ReadHolder guard(lock_);
    queue = dataPointQueue_;
  }

  if (!queue) {
    LOG(ERROR) << "Queue was deleted!";
    GorillaStatsManager::addStatValue(kDataPointQueueDropped);
    reliableDataStartTime_ = time(nullptr);
    return;
  }

  if (!queue->write(std::move(dp))) {
    GorillaStatsManager::addStatValue(kDataPointQueueDropped);
    reliableDataStartTime_ = time(nullptr);
  }
}

void BucketMap::processQueuedDataPoints(bool skipStateCheck) {
  std::shared_ptr<folly::MPMCQueue<QueuedDataPoint>> queue;

  {
    // Take a copy of the shared pointer for the queue. Even if this
    // shard is let go while processing the queue, nothing will cause
    // a segfault and the data points are just skipped.
    folly::RWSpinLock::ReadHolder guard(lock_);
    queue = dataPointQueue_;
  }

  if (!queue) {
    LOG(WARNING) << "Could not process data points. The queue was deleted!";
    return;
  }

  QueuedDataPoint dp;
  while (queue->read(dp)) {
    TimeValuePair value;
    value.unixTime = dp.unixTime;
    value.value = dp.value;

    if (dp.key.length() == 0) {
      // Time series id is known. It's possbible to take a few
      // shortcuts to make adding the data point faster.

      Item item;
      State state;
      {
        folly::RWSpinLock::ReadHolder guard(lock_);
        CHECK(dp.timeSeriesId < rows_.size());
        item = rows_[dp.timeSeriesId];
        state = state_;
      }

      if (!skipStateCheck && state != OWNED && state != PRE_UNOWNED) {
        // Extremely rare corner case. We just set the state to owned
        // and the queue should be really tiny or empty but still
        // state was changed.
        continue;
      }

      putDataPointWithId(
          &item->second, dp.timeSeriesId, value, dp.category, dp.key);
    } else {
      // Run these through the normal workflow.
      put(dp.key, value, dp.category, skipStateCheck);
    }
  }
}

bool BucketMap::putDataPointWithId(
    BucketedTimeSeries* timeSeries,
    uint32_t timeSeriesId,
    const TimeValuePair& value,
    uint16_t category,
    const std::string& key) {
  uint32_t b = bucket(value.unixTime);

  // // The checkpoint should not block insertion.
  // if (FLAGS_is_checkpoint && checkpointFlag_ && !checkpointing_) {
  //   checkpointThread->join();
  //   ++checkpointing_;
  //   checkpointThread.reset(new std::thread([&]() { checkpoint(); }));

  //   // std::future<std::string> strFuSec = std::async(std::launch::async,
  //   // this->checkpoint);
  // }

  bool added;

  // Insert a log entry before the data point inserted.
  if (b >= timeSeries->getCurrentActiveBucket()) {
    if (value.unixTime > timeSeries->getStream().getPreviousTimeStamp()) {
      uint32_t LSN = logWriter_->logData(
          shardId_,
          timeSeriesId,
          value.unixTime,
          value.value,
          FLAGS_log_flag_insertion);
      added = timeSeries->put(b, value, &storage_, timeSeriesId, &category);
    } else {
      UpdateStatusCode ret_status;
      if (!updateDataPoint(key, value, 0, ret_status) &&
          ret_status == UpdateStatusCode::VALUE_MISSING) {
        // Insert a data point into updatedDataMap
        timeSeries->insertIntoDataMap(value, 0);
        added = true;
      } else {
        added = false;
      }
    }
  }
  return added;
}

std::unique_ptr<std::thread> BucketMap::checkpoint() {
  // int64_t maxTimestamp = 0;
  // int64_t t;

  // for(int i=0;i<rows_.size();i++){
  //   t = rows_[i]->second.getStream().getPreviousTimeStamp();
  //   if(t > maxTimestamp)
  //     maxTimestamp = t;
  // }

  // Insert a log entry before finalizing the bucket.
  logWriter_->logCheckpoint(
      shardId_,
      DataLogReader::kStartCheckpoint,
      time(nullptr),
      0,
      FLAGS_log_flag_checkpoint);

  // ALL the LSN of current bucket should be set to zero.
  // clearTimestampExtention();

  resetExtension();

  // Insert a log entry to record the LSN updated time.
  // logWriter_->logCheckpoint(
  //     shardId_,
  //     DataLogReader::kFinishLSNUpdate,
  //     time(nullptr),
  //     0,
  //     FLAGS_log_flag_checkpoint);

  // ALL the bucket except the current bucket should be flush to persistent
  // storages.
  finalizeBuckets();

  std::shared_ptr<std::vector<TimeSeriesStream>> copyOfTSStream(
      new std::vector<TimeSeriesStream>);
  for (int i = 0; i < rows_.size(); i++) {
    copyOfTSStream->emplace_back(rows_[i]->second.getStream());
  }

  std::unique_ptr<std::thread> thread(new std::thread([copyOfTSStream, this]() {
    finalizeStreams(*(copyOfTSStream.get()));

    // Insert a log entry after finalizing the bucket.
    logWriter_->logCheckpoint(
        shardId_,
        DataLogReader::kStopCheckpoint,
        time(nullptr),
        0,
        FLAGS_log_flag_checkpoint);
    // int i = 0;
  }));

  return thread;
}

std::shared_ptr<std::vector<TimeSeriesStream>> BucketMap::copyStreams(){
  std::shared_ptr<std::vector<TimeSeriesStream>> copyOfTSStream(
      new std::vector<TimeSeriesStream>);
  for (int i = 0; i < rows_.size(); i++) {
    copyOfTSStream->emplace_back(rows_[i]->second.getStream());
  }
  return copyOfTSStream;
}

// 重置扩展时间戳，并且将修改过的数据点合并到storage中
bool BucketMap::resetExtension() {
  int i;
  int j;
  TimeSeriesStream data;
  uint16_t dataLength;
  uint16_t itemCount;
  uint32_t timeSeriesId;
  uint32_t pageIndex;
  uint32_t pageOffset;
  BucketStorage::BucketData* buckets = storage_.getBuckets();
  uint8_t numBuckets = storage_.numBuckets();
  BucketStorage::BucketStorageId id;
  uint64_t hash;
  uint32_t index, offset;
  uint16_t length, count;
  std::vector<bool> hasUpdate;
  bool bucketUpdate;
  std::map<int64_t, folly::fbstring>::iterator begin, end;
  for (i = 0; i < numBuckets; i++) {
    BucketStorage::BucketData& bucket = buckets[i];
    std::vector<std::shared_ptr<DataBlock>> pagesCopy;
    // std::unique_lock<std::mutex> guard(bucket.pagesMutex);
    //  folly::RWSpinLock::WriteHolder writeGuard(bucket.fetchLock);
    if (bucket.finalized || bucket.disabled ||
        bucket.position % numBuckets != i)
      continue;
    hasUpdate.resize(bucket.timeSeriesIds.size());
    bucketUpdate = false;
    for (j = 0; j < bucket.timeSeriesIds.size(); j++) {
      timeSeriesId = bucket.timeSeriesIds[j];
      begin = rows_[timeSeriesId]->second.getUpdatedDataMap().lower_bound(
          timestamp(bucket.position));
      end = rows_[timeSeriesId]->second.getUpdatedDataMap().upper_bound(
          timestamp(bucket.position + 1));
      if (begin != end) {
        hasUpdate[timeSeriesId] = true;
        bucketUpdate = true;
      } else
        hasUpdate[timeSeriesId] = false;
    }
    if (!bucketUpdate)
      continue;
    pagesCopy = bucket.pages;
    // folly::RWSpinLock::WriteHolder writeGuard(bucket.fetchLock);
    bucket.activePages = 0;
    bucket.lastPageBytesUsed = 0;
    // bucket.storageIds.clear();
    // bucket.timeSeriesIds.clear();
    bucket.storageIdsLookupMap.clear();
    for (j = 0; j < bucket.pages.size(); j++) {
      bucket.pages[j].reset(new DataBlock);
    }
    for (j = 0; j < bucket.timeSeriesIds.size(); j++) {
      timeSeriesId = bucket.timeSeriesIds[j];
      BucketStorage::parseId(
          bucket.storageIds[j], pageIndex, pageOffset, dataLength, itemCount);
      if (hasUpdate[timeSeriesId]) {
        TimeSeriesStream::flushUpdate(
            pagesCopy[pageIndex]->data + pageOffset,
            dataLength,
            itemCount,
            rows_[timeSeriesId]->second.getUpdatedDataMap(),
            data);
        hash = folly::hash::SpookyHashV2::Hash64(
            data.getDataPtr(), data.size(), 0);
        id = BucketStorage::kInvalidId;
        const auto& matches = bucket.storageIdsLookupMap.equal_range(hash);
        for (auto iter = matches.first; iter != matches.second; ++iter) {
          BucketStorage::parseId(iter->second, index, offset, length, count);
          if (length == data.size() && count == data.count() &&
              (memcmp(
                   data.getDataPtr(),
                   bucket.pages[index]->data + offset,
                   length) == 0)) {
            id = iter->second;
            break;
          }
        }
        if (id == BucketStorage::kInvalidId) {
          if (bucket.activePages == 0 ||
              bucket.lastPageBytesUsed + data.size() >
                  BucketStorage::kPageSize) {
            if (bucket.activePages == bucket.pages.size()) {
              // All allocated pages used, need to allocate more pages.
              if (bucket.pages.size() == kMaxPageCount) {
                LOG(ERROR) << "All pages are already in use.";
                return false;
              }

              bucket.pages.emplace_back(new DataBlock);
            }

            // Use the next page.
            bucket.activePages++;
            bucket.lastPageBytesUsed = 0;
          }

          pageIndex = bucket.activePages - 1;
          pageOffset = bucket.lastPageBytesUsed;
          bucket.lastPageBytesUsed += data.size();

          memcpy(
              bucket.pages[pageIndex]->data + pageOffset,
              data.getDataPtr(),
              data.size());
          id = storage_.createId(
              pageIndex, pageOffset, data.size(), data.count());
          bucket.storageIdsLookupMap.insert(std::make_pair(hash, id));
          data.reset();
        }
      } else {
        hash = folly::hash::SpookyHashV2::Hash64(
            pagesCopy[pageIndex]->data + pageOffset, dataLength, 0);
        id = BucketStorage::kInvalidId;
        const auto& matches = bucket.storageIdsLookupMap.equal_range(hash);
        for (auto iter = matches.first; iter != matches.second; ++iter) {
          BucketStorage::parseId(iter->second, index, offset, length, count);
          if (length == dataLength && count == itemCount &&
              (memcmp(
                   pagesCopy[pageIndex]->data + pageOffset,
                   bucket.pages[index]->data + offset,
                   length) == 0)) {
            id = iter->second;
            break;
          }
        }
        if (id == BucketStorage::kInvalidId) {
          if (bucket.activePages == 0 ||
              bucket.lastPageBytesUsed + dataLength >
                  BucketStorage::kPageSize) {
            if (bucket.activePages == bucket.pages.size()) {
              // All allocated pages used, need to allocate more pages.
              if (bucket.pages.size() == kMaxPageCount) {
                LOG(ERROR) << "All pages are already in use.";
                return false;
              }

              bucket.pages.emplace_back(new DataBlock);
            }

            // Use the next page.
            bucket.activePages++;
            bucket.lastPageBytesUsed = 0;
          }

          index = bucket.activePages - 1;
          offset = bucket.lastPageBytesUsed;
          bucket.lastPageBytesUsed += dataLength;

          memcpy(
              bucket.pages[index]->data + offset,
              pagesCopy[pageIndex]->data + pageOffset,
              dataLength);
          id = storage_.createId(index, offset, dataLength, itemCount);
          bucket.storageIdsLookupMap.insert(std::make_pair(hash, id));
        }
      }
      bucket.storageIds[j] = id;
      rows_[timeSeriesId]->second.getBlocks()[i] = id;
    }
  }

  // 处理stream
  for (i = 0; i < rows_.size(); i++) {
    BucketedTimeSeries& ts = rows_[i]->second;
    // folly::RWSpinLock::WriteHolder writeGuard(ts.getStreamLock());
    begin = ts.getUpdatedDataMap().lower_bound(
        timestamp(ts.getCurrentActiveBucket()));
    end = ts.getUpdatedDataMap().upper_bound(
        timestamp(ts.getCurrentActiveBucket() + 1));
    if (begin != end && ts.getStream().size() != 0) {
      TimeSeriesStream::flushUpdate(
          ts.getStream().getDataPtr(),
          ts.getStream().size(),
          ts.getStream().count(),
          ts.getUpdatedDataMap(),
          data);
      ts.getStream() = std::move(data);
      data.reset();
    }
    ts.getUpdatedDataMap().clear();
  }

  return true;
}

int64_t BucketMap::getReliableDataStartTime() {
  return reliableDataStartTime_;
}

void BucketMap::checkForMissingBlockFiles() {
  // Just look for holes in the progression of files.
  // Gaps between log and block files will be checked elsewhere.

  int missingFiles = 0;
  for (auto it = unreadBlockFiles_.begin();
       std::next(it) != unreadBlockFiles_.end();
       it++) {
    if (*it + 1 != *std::next(it)) {
      missingFiles++;
    }
  }

  if (missingFiles > 0) {
    int32_t now = bucket(time(nullptr));

    std::stringstream error;
    error << missingFiles << " completed block files are missing. Got blocks";
    for (int32_t id : unreadBlockFiles_) {
      error << " " << id;
    }
    error << ". Expected blocks in range [" << now - n_ << ", " << now - 1
          << "]"
          << " for shard " << shardId_;

    LOG(ERROR) << error.str();
    GorillaStatsManager::addStatValue(kDataHoles, missingFiles);
    reliableDataStartTime_ = time(nullptr);
  }
}

int BucketMap::indexDeviatingTimeSeries(
    uint32_t deviationStartTime,
    uint32_t indexingStartTime,
    uint32_t endTime,
    double minimumSigma) {
  if (getState() != OWNED) {
    return 0;
  }

  int totalMinutes = duration(n_) / kGorillaSecondsPerMinute;

  CHECK_EQ(totalMinutes, deviations_.size());

  uint32_t begin = bucket(deviationStartTime);
  uint32_t end = bucket(endTime);

  std::vector<Item> timeSeriesData;
  getEverything(timeSeriesData);

  // Low estimate for the number of time series that have a deviation
  // to avoid constant reallocation.
  int initialSize = timeSeriesData.size() / pow(10, minimumSigma);
  std::vector<std::vector<uint32_t>> deviations(totalMinutes);
  for (int i = indexingStartTime; i <= endTime; i += kGorillaSecondsPerMinute) {
    deviations[i / kGorillaSecondsPerMinute % totalMinutes].reserve(
        initialSize);
  }

  for (int i = 0; i < timeSeriesData.size(); i++) {
    auto& timeSeries = timeSeriesData[i];
    if (!timeSeries.get()) {
      continue;
    }

    std::vector<TimeSeriesBlock> out;
    timeSeries->second.get(
        deviationStartTime, endTime, begin, end, out, getStorage());
    std::vector<TimeValuePair> values;
    for (auto& block : out) {
      TimeSeries::getValues(block, values, deviationStartTime, endTime);
    }

    if (values.size() == 0) {
      continue;
    }

    // Calculate the mean and standard deviation.
    double sum = 0;
    for (auto& v : values) {
      sum += v.value;
    }

    double avg = sum / values.size();
    double variance = 0.0;
    for (auto& value : values) {
      variance += (value.value - avg) * (value.value - avg);
    }
    variance /= values.size();

    if (variance == 0) {
      continue;
    }

    // Index values that are over the limit.
    double stddev = sqrt(variance);
    double limit = minimumSigma * stddev;
    for (auto& v : values) {
      if (v.unixTime >= indexingStartTime && v.unixTime <= endTime &&
          fabs(v.value - avg) >= limit) {
        uint32_t time = (v.unixTime / kGorillaSecondsPerMinute) % totalMinutes;
        deviations[time].push_back(i);
      }
    }
  }

  folly::RWSpinLock::WriteHolder guard(lock_);
  int deviationsIndexed = 0;
  for (int i = indexingStartTime; i <= endTime; i += kGorillaSecondsPerMinute) {
    int pos = i / kGorillaSecondsPerMinute % totalMinutes;
    deviationsIndexed += deviations[pos].size();
    deviations_[pos] = std::move(deviations[pos]);
  }

  return deviationsIndexed;
}

std::vector<BucketMap::Item> BucketMap::getDeviatingTimeSeries(
    uint32_t unixTime) {
  if (getState() != OWNED) {
    return {};
  }

  int totalMinutes = duration(n_) / kGorillaSecondsPerMinute;
  CHECK_EQ(totalMinutes, deviations_.size());

  std::vector<BucketMap::Item> deviations;
  int time = unixTime / kGorillaSecondsPerMinute % totalMinutes;

  folly::RWSpinLock::ReadHolder guard(lock_);
  deviations.reserve(deviations_.size());
  for (auto& row : deviations_[time]) {
    if (row < rows_.size()) {
      deviations.push_back(rows_[row]);
    }
  }

  return deviations;
}
} // namespace gorilla
} // namespace facebook

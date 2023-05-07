/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BeringeiServiceHandler.h"

#include <algorithm>
#include <iostream>

#include <fcntl.h>
#include <sys/prctl.h>

#include <folly/Random.h>
#include <folly/experimental/FunctionScheduler.h>
#include "beringei/lib/BitUtil.h"
#include "beringei/lib/BucketLogWriter.h"
#include "beringei/lib/BucketMap.h"
#include "beringei/lib/BucketStorage.h"
#include "beringei/lib/BucketUtils.h"
#include "beringei/lib/FileUtils.h"
#include "beringei/lib/GorillaStatsManager.h"
#include "beringei/lib/GorillaTimeConstants.h"
#include "beringei/lib/KeyListWriter.h"
#include "beringei/lib/NetworkUtils.h"
#include "beringei/lib/TimeSeries.h"
#include "beringei/lib/Timer.h"

DECLARE_int32(using_libpmem_or_libpmem2);
DECLARE_int32(log_flag_checkpoint);
DECLARE_int32(is_checkpoint);
DECLARE_int32(bucket_size);
DEFINE_int32(pmem_dimms, 8, "Number of pmem dimms");
DEFINE_int32(shards, 8, "Number of maps to use");
DEFINE_int32(buckets, 120, "Number of historical buckets to use");
DEFINE_int32(
    allowed_timestamp_ahead,
    facebook::gorilla::kGorillaSecondsPerMinute,
    "Number of seconds a timestamp is allowed to be ahead of the current time");
DEFINE_int32(
    allowed_timestamp_behind,
    15 * facebook::gorilla::kGorillaSecondsPerMinute,
    "Number of seconds a timestamp is allowed to be behind current time");
DEFINE_string(
    data_directory,
    "/tmp/gorilla_data",
    // "/tmp/gorilla_data_tmp",
    "Directory in which to store time series");
DEFINE_string(
    key_directory,
    "/mnt/ram/ram",
    // "/mnt/ram/",
    // "/mnt/pmem6/",
    "Directory in which to store keys");
DEFINE_string(
    log_directory,
    // "/mnt/ram/ram",
    // "/home/gyy/tmp/tmp",
    "/mnt/ssd1/beringeiData/",
    // "/mnt/ram/",
    // "/mnt/data",
    // "/mnt/pmem",
    // "/mnt/data0/testData/",
    // "/mnt/pmem7/",
    "Directory in which to store logs");
DEFINE_int32(add_shard_threads, 1, "The number of threads for adding shards");
DEFINE_int32(
    key_writer_queue_size,
    50000,
    "The size of queue for each key writer thread. Set this extremely high if "
    "starting the service from scratch with no persistent data.");
DEFINE_int32(key_writer_threads, 1, "The number of key writer threads");
DEFINE_int32(
    log_writer_queue_size,
    300,
    "The size of queue for each log writer thread");

DECLARE_int32(
    log_writer_threads); // number of Threads should be set in DataLog.cpp
// DEFINE_int32(log_writer_threads, 1, "The number of log writer threads");
DEFINE_int32(
    block_writer_threads,
    1,
    "The number of threads for writing completed blocks");
DEFINE_bool(
    create_directories,
    false,
    "Creates data directories for each shard on startup");
DEFINE_int64(
    sleep_between_bucket_finalization_secs,
    600, // 10 min
    "Time to sleep between finalizing buckets");
DEFINE_bool(
    disable_shard_refresh,
    true,
    "Disable shard refresh thread. Primarily used by tests, affects default "
    "shard map ownership assumptions.");

DECLARE_int32(extention_len);

namespace facebook {
namespace gorilla {

const static std::string kPurgedTimeSeries = ".purged_time_series";
const static std::string kPurgedTimeSeriesInCategoryPrefix =
    ".purged_time_series_in_category_";
const int kPurgeInterval = facebook::gorilla::kGorillaSecondsPerHour;
const static std::string kMsPerKeyListCompact = ".ms_per_key_list_compact";
const int kCleanInterval = 6 * facebook::gorilla::kGorillaSecondsPerHour;
const static std::string kTooSlowToFinalizeBuckets =
    ".too_slow_to_finalize_buckets";
const static std::string kMsPerFinalizeShardBucket =
    ".ms_per_finalize_shard_bucket";
const static std::string kUsPerGet = ".us_per_get";
const static std::string kUsPerGetPerKey = ".us_per_get_per_key";
const static std::string kUsPerPut = ".us_per_put";
const static std::string kUsPerPutPerKey = ".us_per_put_per_key";
const static std::string kKeysPut = ".keys_put";
const static std::string kKeysGot = ".keys_got";
static const std::string kMissingTooMuchData = ".status_missing_too_much_data";
const static std::string kNewKeys = ".new_keys";
const static std::string kDatapointsAdded = ".datapoints_added";
const static std::string kDatapointsDropped = ".datapoints_dropped";
const static std::string kDatapointsBehind = ".datapoints_behind";
const static std::string kDatapointsAhead = ".datapoints_ahead";
const static std::string kDatapointsNotOwned = ".datapoints_not_owned";
const static std::string kTooLongKeys = ".too_long_keys";
const static std::string kNewTimeSeriesBlocked = ".new_time_series_blocked";
const static std::string kNumShards = ".num_shards";
const static std::string kMsPerShardAdd = ".ms_per_shard_add";
const static std::string kShardsAdded = ".shards_added";
const static std::string kShardsBeingAdded = ".shards_being_added";
const static std::string kShardsDropped = ".shards_dropped";
const static std::string kUsPerGetLastUpdateTimes =
    ".us_per_get_last_update_times";

// Max size for ODS key is 256 and entity 128. This will fit those and
// some extra characteFLAGS_shardsrs.
const int kMaxKeyLength = 400;
const int kRefreshShardMapInterval = 60; // poll every minute

BeringeiServiceHandler::BeringeiServiceHandler(
    std::shared_ptr<BeringeiConfigurationAdapterIf> configAdapter,
    std::shared_ptr<MemoryUsageGuardIf> memoryUsageGuard,
    const std::string& serviceName,
    const int32_t port)
    : shards_(FLAGS_shards, FLAGS_add_shard_threads),
      configAdapter_(std::move(configAdapter)),
      memoryUsageGuard_(std::move(memoryUsageGuard)),
      serviceName_(serviceName),
      port_(port),
      requireCheckpoint(false) {
  // the number of threads for each thread pool must exceed 0
  CHECK_GT(fLI::FLAGS_key_writer_threads, 0);
  CHECK_GT(fLI::FLAGS_log_writer_threads, 0);
  CHECK_GT(fLI::FLAGS_block_writer_threads, 0);

  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  pthread_rwlockattr_setkind_np(
      &attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&insertTxRwlock_, &attr);
  pthread_rwlock_init(&updateTxRwlock_, &attr);
  pthread_rwlockattr_destroy(&attr);

  if (!FLAGS_create_directories) {
    std::string shardZero = FileUtils::joinPaths(FLAGS_data_directory, "0");
    if (!FileUtils::isDirectory(shardZero)) {
      LOG(FATAL) << "Data directory '" << shardZero << " does not exist. "
                 << "If you are running beringei for the first time, "
                 << "please pass --create_directories flag.";
    }
    std::string keyZero = FileUtils::joinPaths(FLAGS_key_directory, "0");
    if (!FileUtils::isDirectory(keyZero)) {
      LOG(FATAL) << "Data directory '" << keyZero << " does not exist. "
                 << "If you are running beringei for the first time, "
                 << "please pass --create_directories flag.";
    }
    std::string logZero = FileUtils::joinPaths(FLAGS_log_directory, "0");
    if (!FileUtils::isDirectory(logZero)) {
      LOG(FATAL) << "Log directory '" << logZero << " does not exist. "
                 << "If you are running beringei for the first time, "
                 << "please pass --create_directories flag.";
    }
  }

  // start monitoring
  FileUtils::startMonitoring();
  BucketLogWriter::startMonitoring();
  BucketMap::startMonitoring();
  KeyListWriter::startMonitoring();
  BucketStorage::startMonitoring();

  BucketLogWriter::setNumShards(FLAGS_shards);

  std::vector<std::shared_ptr<KeyListWriter>> keyWriters;
  for (int i = 0; i < FLAGS_key_writer_threads; i++) {
    if (FLAGS_using_libpmem_or_libpmem2) {
      std::string curKeyDirectory = FLAGS_key_directory;
      curKeyDirectory.append(std::to_string(i % FLAGS_pmem_dimms));
      keyWriters.emplace_back(
          new KeyListWriter(curKeyDirectory, FLAGS_key_writer_queue_size));
    } else {
      keyWriters.emplace_back(
          new KeyListWriter(FLAGS_data_directory, FLAGS_key_writer_queue_size));
    }
  }

  // // Initialize snapshot for parallel logging.
  // DataLogWriter::snapshot DataLogWriter::snapshot_ =
  // DataLogWriter::snapshot(FLAGS_log_writer_threads);
  // std::vector<folly::RWSpinLock>
  // DataLogWriter::locks_(FLAGS_log_writer_threads);

  // Initialize bucketLogWriters.
  std::vector<std::shared_ptr<BucketLogWriter>> bucketLogWriters;
  for (int i = 0; i < FLAGS_log_writer_threads; i++) {
    if (FLAGS_using_libpmem_or_libpmem2) {
      std::string curLogDirectory = FLAGS_log_directory;
      curLogDirectory.append(std::to_string(i % FLAGS_pmem_dimms));
      // curLogDirectory.append("/logData");
      bucketLogWriters.emplace_back(new BucketLogWriter(
          FLAGS_bucket_size,
          curLogDirectory,
          FLAGS_log_writer_queue_size,
          FLAGS_allowed_timestamp_behind,
          i));
    } else {
      bucketLogWriters.emplace_back(new BucketLogWriter(
          FLAGS_bucket_size,
          FLAGS_log_directory,
          FLAGS_log_writer_queue_size,
          FLAGS_allowed_timestamp_behind,
          i));
    }
  }

  srandom(folly::randomNumberSeed());

  std::vector<std::unique_ptr<BucketMap>> maps(FLAGS_shards);

  #pragma omp parallel
  #pragma omp for
  for (int i = 0; i < FLAGS_shards; i++) {
    // Select the bucket log writer and block writer for each shard by
    // random instead of by modulo to allow better distribution
    // because sharding algorithm used by Shard Manager is
    // unknown. The distribution doesn't have to be even. As long as
    // it's somewhat distributed it should be fine.
    // auto keyWriter = keyWriters[random() % keyWriters.size()];
    // auto bucketLogWriter = bucketLogWriters[random() %
    // bucketLogWriters.size()];
    auto keyWriter = keyWriters[i % keyWriters.size()];
    auto bucketLogWriter = bucketLogWriters[i % bucketLogWriters.size()];
    auto map = folly::make_unique<BucketMap>(
        FLAGS_buckets,
        FLAGS_bucket_size,
        i,
        FLAGS_data_directory,
        keyWriter,
        bucketLogWriter,
        BucketMap::UNOWNED);

    if (FLAGS_create_directories) {
      FileUtils utils(i, "", FLAGS_data_directory);
      utils.createDirectories();
      FileUtils utils_log(i, "", FLAGS_log_directory);
      utils_log.createDirectories();
    }

    // If we won't be refreshing the shard map, then assume we own everything.
    // Otherwise, default to owning nothing.
    if (fLB::FLAGS_disable_shard_refresh) {
      LOG(INFO) << "Running with shard refresh disabled, "
                << "defaulting to owning all shards";
      map->setState(BucketMap::PRE_OWNED);
      map->readKeyList();
      map->findBlocks();
      while (map->readBlockFiles()) {
        // Nothing here...
      }
      map->readStreams();
      map->readData();
    }
    shards_.initialize(i, std::move(map));
  }

  // If we should be refreshing from a shard map, read the config and add shards
  // we should own.
  if (!fLB::FLAGS_disable_shard_refresh) {
    refreshShardConfig();
    LOG(INFO) << "Successfully read shard config for the first time!";
  }

  purgeThread_.addFunction(
      std::bind(&BeringeiServiceHandler::purgeThread, this),
      std::chrono::seconds(kPurgeInterval),
      "Purge Thread",
      std::chrono::seconds(kPurgeInterval));
  // purgeThread_.start();

  cleanThread_.addFunction(
      std::bind(&BeringeiServiceHandler::cleanThread, this),
      std::chrono::seconds(kCleanInterval),
      "Clean Thread",
      std::chrono::seconds(kCleanInterval));
  // cleanThread_.start();

  // Bucket finalizer thread runs at an interval slightly less than two hours.
  // We wait for a cycle before actually starting the thread to allow shards to
  // be loaded first before trying to finalize anything.
  bucketFinalizerThread_.addFunction(
      std::bind(&BeringeiServiceHandler::finalizeBucketsThread, this),
      std::chrono::seconds(FLAGS_sleep_between_bucket_finalization_secs),
      "Bucket Finalizer Thread",
      std::chrono::seconds(FLAGS_sleep_between_bucket_finalization_secs));
  // bucketFinalizerThread_.start();

  checkpointThread.reset(new std::thread([this]() {
    prctl(PR_SET_NAME, "checkpoint");
    while (true) {
      checkpoint();
    }
  }));

  if (!FLAGS_disable_shard_refresh) {
    refreshShardConfigThread_.addFunction(
        std::bind(&BeringeiServiceHandler::refreshShardConfig, this),
        std::chrono::seconds(kRefreshShardMapInterval),
        "Refresh Shard Map Thread",
        std::chrono::seconds(kRefreshShardMapInterval));
    refreshShardConfigThread_.start();
  }

  GorillaStatsManager::addStatExportType(kUsPerGet, AVG);
  GorillaStatsManager::addStatExportType(kUsPerGet, COUNT);
  GorillaStatsManager::addStatExportType(kUsPerGetPerKey, AVG);
  GorillaStatsManager::addStatExportType(kUsPerPut, AVG);
  GorillaStatsManager::addStatExportType(kUsPerPut, COUNT);
  GorillaStatsManager::addStatExportType(kUsPerPutPerKey, AVG);

  GorillaStatsManager::addStatExportType(kKeysPut, AVG);
  GorillaStatsManager::addStatExportType(kKeysPut, SUM);

  GorillaStatsManager::addStatExportType(kKeysGot, AVG);
  GorillaStatsManager::addStatExportType(kKeysGot, SUM);

  GorillaStatsManager::addStatExportType(kNewKeys, SUM);
  GorillaStatsManager::addStatExportType(kDatapointsAdded, SUM);
  GorillaStatsManager::addStatExportType(kDatapointsDropped, SUM);
  GorillaStatsManager::addStatExportType(kDatapointsBehind, SUM);
  GorillaStatsManager::addStatExportType(kDatapointsAhead, SUM);
  GorillaStatsManager::addStatExportType(kNewTimeSeriesBlocked, SUM);
  GorillaStatsManager::addStatExportType(kPurgedTimeSeries, SUM);

  GorillaStatsManager::addStatExportType(kMsPerFinalizeShardBucket, AVG);
  GorillaStatsManager::addStatExportType(kMsPerFinalizeShardBucket, COUNT);
  GorillaStatsManager::addStatExportType(kTooSlowToFinalizeBuckets, SUM);

  GorillaStatsManager::addStatExportType(kMsPerKeyListCompact, AVG);
  GorillaStatsManager::addStatExportType(kMsPerKeyListCompact, COUNT);

  GorillaStatsManager::addStatExportType(kDatapointsNotOwned, SUM);
  GorillaStatsManager::addStatExportType(kMissingTooMuchData, SUM);

  GorillaStatsManager::addStatExportType(kUsPerGetLastUpdateTimes, AVG);
  GorillaStatsManager::addStatExportType(kUsPerGetLastUpdateTimes, COUNT);
}

BeringeiServiceHandler::~BeringeiServiceHandler() {
  purgeThread_.shutdown();
  cleanThread_.shutdown();
  bucketFinalizerThread_.shutdown();
  refreshShardConfigThread_.shutdown();
}

void BeringeiServiceHandler::putDataPoints(
    PutDataResult& response,
    std::unique_ptr<PutDataRequest> req) {
  Timer timer(true);

  int newTimeSeries = 0;
  int datapointsAdded = 0;
  auto now = time(nullptr);
  int notOwned = 0;
  int newTimeSeriesBlocked = 0;

  for (auto& dp : req->data) {
    auto originalUnixTime = dp.value.unixTime;

    // Set time 0 to now.
    if (dp.value.unixTime == 0) {
      dp.value.unixTime = now;
    }

    // if (dp.value.unixTime < now - FLAGS_allowed_timestamp_behind) {
    //   dp.value.unixTime = now;
    //   GorillaStatsManager::addStatValue(kDatapointsBehind);
    // }

    // if (dp.value.unixTime > now + FLAGS_allowed_timestamp_ahead) {
    //   dp.value.unixTime = now;
    //   GorillaStatsManager::addStatValue(kDatapointsAhead);
    // }

    if (dp.key.key.length() > kMaxKeyLength) {
      GorillaStatsManager::addStatValue(kTooLongKeys);
      continue;
    }

    auto map = shards_.getShardMap(dp.key.shardId);
    if (!map) {
      continue;
    }

    if (map->get(dp.key.key) == nullptr &&
        memoryUsageGuard_->weAreLowOnMemory()) {
      ++newTimeSeriesBlocked;
      continue;
    }

    // The put call will do the check for the shard ownership
    auto ret = map->put(dp.key.key, dp.value, dp.categoryId);

    if (ret.first == BucketMap::kNotOwned) {
      dp.value.unixTime = originalUnixTime;
      response.data.push_back(dp);
      notOwned++;
    } else {
      newTimeSeries += ret.first;
      datapointsAdded += ret.second;
    }
  }

  GorillaStatsManager::addStatValue(kUsPerPut, timer.get());
  GorillaStatsManager::addStatValue(
      kUsPerPutPerKey, timer.get() / (double)req->data.size());
  GorillaStatsManager::addStatValue(kKeysPut, req->data.size());
  GorillaStatsManager::addStatValue(kNewKeys, newTimeSeries);
  GorillaStatsManager::addStatValue(kDatapointsAdded, datapointsAdded);
  GorillaStatsManager::addStatValue(
      kDatapointsDropped, req->data.size() - datapointsAdded);
  GorillaStatsManager::addStatValue(kDatapointsNotOwned, notOwned);
  GorillaStatsManager::addStatValue(
      kNewTimeSeriesBlocked, newTimeSeriesBlocked);
}

void BeringeiServiceHandler::getData(
    GetDataResult& ret,
    std::unique_ptr<GetDataRequest> req) {
  Timer timer(true);
  ret.results.resize(req->keys.size());
  int keysFound = 0;

  for (int i = 0; i < req->keys.size(); i++) {
    const Key& key = req->keys[i];
    auto map = shards_.getShardMap(key.shardId);
    if (!map || key.key.length() > kMaxKeyLength) {
      ret.results[i].status = StatusCode::KEY_MISSING;
      continue;
    }

    BucketMap::State state = map->getState();
    if (state == BucketMap::UNOWNED) {
      // Not owning this shard, caller has stale shard information.
      ret.results[i].status = StatusCode::DONT_OWN_SHARD;
    } else if (
        state >= BucketMap::PRE_OWNED &&
        state < BucketMap::READING_BLOCK_DATA) {
      // Not ready to serve reads yet.
      ret.results[i].status = StatusCode::SHARD_IN_PROGRESS;
    } else {
      auto row = map->get(key.key);
      if (row.get()) {
        keysFound++;
        LOG(INFO) << "Target key is: " << key.key << std::endl;
        row->second.get(
            req->begin,
            req->end,
            shards_[0]->bucket(req->begin),
            shards_[0]->bucket(req->end),
            ret.results[i].data,
            map->getStorage());
        row->second.setQueried();
        if (state == BucketMap::READING_BLOCK_DATA) {
          // Some of the data hasn't been read yet. Let the client
          // decide what to do with the results, i.e., ask the other
          // coast if possible.
          ret.results[i].status = StatusCode::SHARD_IN_PROGRESS;
        } else if (req->begin < map->getReliableDataStartTime()) {
          ret.results[i].status = StatusCode::MISSING_TOO_MUCH_DATA;
          GorillaStatsManager::addStatValue(kMissingTooMuchData, 1);
        } else {
          ret.results[i].status = StatusCode::OK;
        }
      } else {
        // There's no such key.
        ret.results[i].status = StatusCode::KEY_MISSING;
      }
    }

    if (ret.results[i].status != StatusCode::OK) {
      LOG(INFO) << "Bucket map state is: " << state << std::endl;
      LOG(INFO) << "Return state is: " << (int)(ret.results[i].status)
                << std::endl;
    }
  }

  // uint64_t bitPos = 0;
  // auto curData = ret.results[0].data[0];
  // uint64_t unixTime =
  // BitUtil::readValueFromBitString(curData.get_data().c_str(), bitPos, 64);

  // LOG(INFO) << "Target timestamp is: " << unixTime << std::endl;

  // uint64_t value =
  // BitUtil::readValueFromBitString(curData.get_data().c_str(), bitPos, 64);
  // double lastValue = *(double*)(&value);

  // LOG(INFO) << "Target value is: " << lastValue << std::endl;

  // // Creat updatedDataMap by the first block.
  // // The key is timestamp, the value is the value of each data point.
  // std::map<int64_t, double> updatedDataMap;
  // auto& mapBlock = curData;
  // TimeSeries::createMap(mapBlock, &updatedDataMap);

  // auto it = updatedDataMap.begin();

  // LOG(INFO) << "Timestamp is: " << it->first << std::endl;
  // LOG(INFO) << "Value is: " << it->second << std::endl;

  // curData = ret.results[0].data[1];
  // uint64_t previousValue = 0;
  // uint64_t previousLeadingZeros = 0;
  // uint64_t previousTrailingZeros = 0;
  // bitPos = 0;
  // int64_t previousTimestampDelta = 60U;

  // int64_t firstTimestamp = BitUtil::readValueFromBitString(
  //     curData.get_data().c_str(), bitPos, 31U);

  // uint64_t nonZeroValue =
  // BitUtil::readValueFromBitString(curData.get_data().c_str(), bitPos, 1);

  // if (!nonZeroValue) {
  //   value = *(double*)&previousValue;
  // }
  // else {
  //   uint64_t usePreviousBlockInformation =
  //     BitUtil::readValueFromBitString(curData.get_data().c_str(), bitPos, 1);

  //   uint64_t xorValue;
  //   if (usePreviousBlockInformation) {
  //     xorValue = BitUtil::readValueFromBitString(
  //         curData.get_data().c_str(), bitPos, 64 - previousLeadingZeros -
  //         previousTrailingZeros);
  //     xorValue <<= previousTrailingZeros;
  //   } else {
  //     uint64_t leadingZeros =
  //         BitUtil::readValueFromBitString(curData.get_data().c_str(), bitPos,
  //         5U);
  //     uint64_t blockSize =
  //         BitUtil::readValueFromBitString(curData.get_data().c_str(), bitPos,
  //         6U) + 1U;

  //     previousTrailingZeros = 64 - blockSize - leadingZeros;
  //     xorValue = BitUtil::readValueFromBitString(curData.get_data().c_str(),
  //     bitPos, blockSize); xorValue <<= previousTrailingZeros;
  //     previousLeadingZeros = leadingZeros;
  //   }

  //   uint64_t value = xorValue ^ previousValue;
  //   previousValue = value;

  //   value = *(double*)&value;
  // }

  // int64_t previousTimestamp = firstTimestamp;

  // // Read the tombstone bit.
  // uint64_t tombstone =
  // BitUtil::readValueFromBitString(curData.get_data().c_str(), bitPos, 1);
  // // Read the lock bit which is useless for the query.
  // BitUtil::readValueFromBitString(curData.get_data().c_str(), bitPos, 1);

  // LOG(INFO) << "Timestamp is " << firstTimestamp << std::endl;
  // LOG(INFO) << "The original value is " << value << std::endl;
  // LOG(INFO) << "The tombstone is " << tombstone << std::endl;

  // if (tombstone) {
  //   std::map<int64_t, double>::const_iterator it = updatedDataMap.begin();
  //   auto curIt = updatedDataMap.find(firstTimestamp);
  //   // Update the value if the data point exists in the updatedDataMap.
  //   if (curIt != updatedDataMap.end()) {
  //     value = curIt->second;
  //   }

  //   // Output data points which exist just in the updatedDataMap.
  //   // The code works when data points in block and updatedDataMap
  //   // are both in order.
  //   while(it != curIt && it != updatedDataMap.end()) {
  //     if (it->first >= 1438000000) {
  //       if (it->first < FLAGS_gorilla_blacklisted_time_min ||
  //           it->first > FLAGS_gorilla_blacklisted_time_max) {
  //         LOG(INFO) << "Deleted value!!!!! "<< std::endl;
  //       }
  //     }
  //     ++it;
  //   }
  //   it = ++curIt;
  // }

  // std::vector<std::pair<Key, std::vector<TimeValuePair>>> curResult(1);

  // TimeSeries::getValues(
  // ret.results[0].data[1], curResult[0].second, 1438000000, 1438000000,
  // &updatedDataMap);

  GorillaStatsManager::addStatValue(kUsPerGet, timer.get());
  GorillaStatsManager::addStatValue(
      kUsPerGetPerKey, timer.get() / (double)req->keys.size());
  GorillaStatsManager::addStatValue(kKeysGot, keysFound);
}

// written by yls
// 读取扩展时间戳
bool BeringeiServiceHandler::getExtension(
    const Key& key,
    int64_t timeBegin,
    int64_t timeEnd,
    std::vector<std::pair<int64_t, uint64_t>>& out) {
  auto map = shards_.getShardMap(key.shardId);
  if (!map || key.key.length() > kMaxKeyLength) {
    return false;
  }
  BucketMap::State state = map->getState();
  if (state == BucketMap::UNOWNED) {
    // Not owning this shard, caller has stale shard information.
    return false;
  } else if (
      state >= BucketMap::PRE_OWNED && state < BucketMap::READING_BLOCK_DATA) {
    // Not ready to serve reads yet.
    return false;
  } else {
    auto row = map->get(key.key);
    if (row.get()) {
      row->second.getExtension(timeBegin, timeEnd, out);
    }
  }
  return true;
}

void BeringeiServiceHandler::updateDataPoints(
    UpdateDataPointsResult& ret,
    std::unique_ptr<UpdateDataPointsRequest> req) {
  // The number of data points in requests.
  int numOfDataPoints = req->data.size();
  // LOG(INFO) << "wanwanvv: numOfDataPoints=" << numOfDataPoints;
  // LOG(INFO) << "wanwanvv: key=" << req->data[0].get_key().get_key() <<
  // req->data[0].get_key().get_shardId(); LOG(INFO) << "wanwanvv: time=" <<
  // req->data[0].get_value().get_unixTime() << " value=" <<
  // req->data[0].get_value().get_value(); The number of updated data points.
  int numOfUpdatedPoints = 0;
  ret.data.resize(numOfDataPoints);
  // Timer timer(true);

  // auto now = time(nullptr);

  // Process each data point.
  for (int i = 0; i < numOfDataPoints; ++i) {
    const auto& dp = req->data[i];

    // Initialize the result.
    UpdateDataPointsResultStruct& ret_dp = ret.data[i];

    ret_dp.key = dp.key;
    ret_dp.value = dp.value;
    ret_dp.categoryId = dp.categoryId;
    if (dp.key.key.length() > kMaxKeyLength) {
      ret_dp.status = UpdateStatusCode::KEY_OVERMAX;
      continue;
    }
    // Does not support updating data points which have flushed to disks.
    //  if(update_bucket<=data_[0]->bucket(now)-FLAGS_buckets){
    //    ret_dp.status=UpdateStatusCode::OVER_TIME;
    //    continue;
    //  }

    // Get the shard in which the data point is.
    auto map = getShardMap(dp.key.shardId);
    if (!map) {
      ret_dp.status = UpdateStatusCode::DONT_OWN_SHARD;
      continue;
    }
    if (memoryUsageGuard_->weAreLowOnMemory()) {
      ret_dp.status = UpdateStatusCode::MEMORY_LOW;
      continue;
    }

    if (map->updateDataPoint(
            dp.key.key, dp.value, dp.categoryId, ret_dp.status)) {
      ++numOfUpdatedPoints;
    }
    if (ret_dp.status != UpdateStatusCode::OK_ALL) {
      LOG(INFO) << "ret=" << (int)ret_dp.status;
    }
    // LOG(INFO) << "wanwanvv: ret=" << (int)ret_dp.status;
  }
  // LOG(INFO) << "wanwanvv: numOfUpdatedPoints=" << numOfUpdatedPoints;
  return;
}

void BeringeiServiceHandler::deleteDataPoints(
    DeleteDataPointsResult& ret,
    std::unique_ptr<DeleteDataPointsRequest> req) {
  // The number of data points in requests.
  int numOfDataPoints = req->data.size();
  // LOG(INFO) << "wanwanvv: numOfDataPoints=" << numOfDataPoints;
  //  The number of updated data points.
  int numOfDeletedPoints = 0;
  ret.data.resize(numOfDataPoints);
  // Timer timer(true);
  // Process each data point.
  for (int i = 0; i < numOfDataPoints; ++i) {
    const auto& dp = req->data[i];

    // Initialize the result.
    DeleteDataPointsResultStruct& ret_dp = ret.data[i];

    ret_dp.key = dp.key;
    ret_dp.value = dp.value;
    ret_dp.categoryId = dp.categoryId;
    if (dp.key.key.length() > kMaxKeyLength) {
      ret_dp.status = DeleteStatusCode::KEY_OVERMAX;
      continue;
    }

    // Get the shard in which the data point is.
    auto map = getShardMap(dp.key.shardId);
    if (!map) {
      ret_dp.status = DeleteStatusCode::DONT_OWN_SHARD;
      continue;
    }

    if (map->deleteDataPoint(
            dp.key.key, dp.value, dp.categoryId, ret_dp.status)) {
      ret_dp.status = DeleteStatusCode::OK_ALL;
      ++numOfDeletedPoints;
    }
  }
  // LOG(INFO) << "wanwanvv: numOfDeletedPoints=" << numOfDeletedPoints;
  return;
}

// written by yls
// 提交事务
void BeringeiServiceHandler::doTransaction(
    TxResult& ret,
    std::unique_ptr<TxRequest> req) {
  int i;
  bool extensionOverflow;
  std::vector<TxRequestOp>& op = req->op;
  std::vector<std::pair<int64_t, uint64_t>> extensions;
  std::vector<std::pair<int64_t, uint64_t>> validateExtensions;
  int64_t begin, end;

  // if (op.size() == 1 && op[0].get_key().get_key() == "/checkpoint") {
  //   {
  //     std::unique_lock<std::mutex> lock(checkpointMutex_);
  //     if (!requireCheckpoint) {
  //       requireCheckpoint = true;
  //       checkpointReadyCv_.notify_one();
  //     }
  //     while (requireCheckpoint) {
  //       checkpointCompleteCv_.wait(lock);
  //     }
  //   }
  //   LOG(INFO) << "execute checkpoint";
  //   ret.set_status(TxStatusCode::OK);
  //   return;
  // }

  bool isInsertTx = true;

  for (i = 0; i < op.size(); i++) {
    if (op[i].opcode != TxOpCode::INSERT) {
      isInsertTx = false;
      break;
    }
  }

  if (isInsertTx)
    pthread_rwlock_rdlock(&insertTxRwlock_);
  else
    pthread_rwlock_rdlock(&updateTxRwlock_);
  // 判断扩展时间戳是否溢出
  if (!isInsertTx) {
    while (true) {
      for (i = 0; i < op.size(); i++) {
        TxRequestOp& opInTx = op[i];
        if (opInTx.opcode == TxOpCode::UPDATE ||
            opInTx.opcode == TxOpCode::DELETE) {
          begin = opInTx.get_value().get_unixTime();
          end = begin;
          if (!getExtension(opInTx.get_key(), begin, end, extensions)) {
            pthread_rwlock_unlock(&updateTxRwlock_);
            LOG(INFO) << "get extension fail!";
            ret.set_status(TxStatusCode::FAIL);
            return;
          }
        }
      }
      extensionOverflow = false;
      for (i = 0; i < extensions.size(); i++) {
        if (extensions[i].second + op.size() >= (1 << FLAGS_extention_len)  && FLAGS_is_checkpoint) {
          pthread_rwlock_unlock(&updateTxRwlock_);
          extensionOverflow = true;
          {
            std::unique_lock<std::mutex> lock(checkpointMutex_);
            if (!requireCheckpoint) {
              requireCheckpoint = true;
              checkpointReadyCv_.notify_one();
            }
            while (requireCheckpoint) {
              checkpointCompleteCv_.wait(lock);
            }
          }
          pthread_rwlock_rdlock(&updateTxRwlock_);
          break;
        }
      }
      if (!extensionOverflow)
        break;
      extensions.clear();
    }
  }

  // 读阶段，遍历事务中每一个操作
  if (!isInsertTx) {
    for (i = 0; i < op.size(); i++) {
      TxRequestOp& opInTx = op[i];
      if (opInTx.opcode == TxOpCode::READ || opInTx.opcode == TxOpCode::SCAN) {
        if (opInTx.opcode == TxOpCode::READ) {
          begin = opInTx.get_value().get_unixTime();
          end = begin;
        } else {
          begin = opInTx.get_startTime();
          end = opInTx.get_endTime();
        }
        if (!getExtension(opInTx.get_key(), begin, end, extensions)) {
          pthread_rwlock_unlock(&updateTxRwlock_);
          LOG(INFO) << "get extension fail!";
          ret.set_status(TxStatusCode::FAIL);
          return;
        }
      }
    }
  }

  // 获取锁
  if (!isInsertTx && !lockDataPointsInOp(op)) {
    pthread_rwlock_unlock(&updateTxRwlock_);
    LOG(INFO) << "get lock fail!";
    ret.set_status(TxStatusCode::FAIL);
    return;
  }

  // 验证阶段
  if (!isInsertTx) {
    for (i = 0; i < op.size(); i++) {
      TxRequestOp& opInTx = op[i];
      if (opInTx.opcode == TxOpCode::UPDATE ||
          opInTx.opcode == TxOpCode::DELETE) {
        begin = opInTx.get_value().get_unixTime();
        end = begin;
        if (!getExtension(opInTx.get_key(), begin, end, validateExtensions)) {
          pthread_rwlock_unlock(&updateTxRwlock_);
          LOG(INFO) << "get extension fail!";
          ret.set_status(TxStatusCode::FAIL);
          return;
        }
      }
    }
    for (i = 0; i < op.size(); i++) {
      TxRequestOp& opInTx = op[i];
      if (opInTx.opcode == TxOpCode::READ || opInTx.opcode == TxOpCode::SCAN) {
        if (opInTx.opcode == TxOpCode::READ) {
          begin = opInTx.get_value().get_unixTime();
          end = begin;
        } else {
          begin = opInTx.get_startTime();
          end = opInTx.get_endTime();
        }
        if (!getExtension(opInTx.get_key(), begin, end, validateExtensions)) {
          pthread_rwlock_unlock(&updateTxRwlock_);
          LOG(INFO) << "get extension fail!";
          ret.set_status(TxStatusCode::FAIL);
          return;
        }
      }
    }
    if (extensions.size() != validateExtensions.size()) {
      LOG(INFO) << "validate extension size fail!";
      unlockDataPointsInOp(op);
      pthread_rwlock_unlock(&updateTxRwlock_);
      ret.set_status(TxStatusCode::FAIL);
      return;
    }

    for (i = 0; i < extensions.size(); i++) {
      if (extensions[i] != validateExtensions[i]) {
        LOG(INFO) << "validate extension fail!";
        unlockDataPointsInOp(op);
        pthread_rwlock_unlock(&updateTxRwlock_);
        ret.set_status(TxStatusCode::FAIL);
        return;
      }
    }
  }

  // 写阶段
  PutDataResult insertRet;
  std::unique_ptr<PutDataRequest> insertReq;
  UpdateDataPointsResult updateRet;
  std::unique_ptr<UpdateDataPointsRequest> updateReq;
  DeleteDataPointsResult deleteRet;
  std::unique_ptr<DeleteDataPointsRequest> deleteReq;
  for (i = 0; i < op.size(); i++) {
    switch (op[i].get_opcode()) {
      case TxOpCode::INSERT:
        insertReq.reset(new PutDataRequest);
        insertReq->data.emplace_back();
        insertReq->data.back().key = op[i].get_key();
        insertReq->data.back().__isset.key = true;
        insertReq->data.back().value = op[i].get_value();
        insertReq->data.back().__isset.value = true;
        insertReq->data.back().set_categoryId(0);
        putDataPoints(insertRet, std::move(insertReq));
        break;
      case TxOpCode::UPDATE:
        updateReq.reset(new UpdateDataPointsRequest);
        updateReq->data.emplace_back();
        updateReq->data.back().key = op[i].get_key();
        updateReq->data.back().__isset.key = true;
        updateReq->data.back().value = op[i].get_value();
        updateReq->data.back().__isset.value = true;
        updateReq->data.back().set_categoryId(0);
        updateDataPoints(updateRet, std::move(updateReq));
        break;
      case TxOpCode::DELETE:
        deleteReq.reset(new DeleteDataPointsRequest);
        deleteReq->data.emplace_back();
        deleteReq->data.back().key = op[i].get_key();
        deleteReq->data.back().__isset.key = true;
        deleteReq->data.back().value = op[i].get_value();
        deleteReq->data.back().__isset.value = true;
        deleteReq->data.back().set_categoryId(0);
        deleteDataPoints(deleteRet, std::move(deleteReq));
        break;
    }
  }
  // 释放锁
  unlockDataPointsInOp(op);

  if (isInsertTx)
    pthread_rwlock_unlock(&insertTxRwlock_);
  else
    pthread_rwlock_unlock(&updateTxRwlock_);

  ret.set_status(TxStatusCode::OK);
  return;
}

// written by yls
// 合并区间
void BeringeiServiceHandler::mergeInterval(
    std::vector<std::pair<int64_t, int64_t>>& intervals,
    std::vector<std::pair<int64_t, int64_t>>& ret) {
  sort(intervals.begin(), intervals.end()); // 对数组起始坐标进行升序排序
  int64_t st = -2e9,
          ed = -2e9; // ed代表区间结尾，st代表区间开头（范围为负无穷）
  for (const auto& s : intervals) {
    if (ed < s.first) { // 两个区间没有交集
      if (st != -2e9)
        ret.push_back({st, ed});
      st = s.first, ed = s.second; // 维护区间2
    } else
      ed = std::max(
          ed, s.second); // 两个区间有交集时，右侧坐标取两个区间中相对大的坐标
  }
  if (st != -2e9)
    ret.push_back({st, ed});
}

// written by yls
// 为事务中的每个操作访问的数据上锁
bool BeringeiServiceHandler::lockDataPointsInOp(std::vector<TxRequestOp>& op) {
  std::unordered_map<Key, std::vector<std::pair<int64_t, int64_t>>, KeyHash>
      intervals;
  std::unordered_map<Key, std::vector<std::pair<int64_t, int64_t>>, KeyHash>
      disjointintervals;
  int i, j;
  int64_t begin_time;
  int64_t end_time;

  for (i = 0; i < op.size(); i++) {
    if (op[i].get_opcode() == TxOpCode::INSERT)
      continue;
    if (op[i].get_opcode() == TxOpCode::SCAN) {
      begin_time = op[i].get_startTime();
      end_time = op[i].get_endTime();
    } else {
      begin_time = op[i].get_value().get_unixTime();
      end_time = begin_time;
    }
    intervals[op[i].get_key()].emplace_back(begin_time, end_time + 1);
  }

  auto it = intervals.begin();
  for (; it != intervals.end(); it++) {
    mergeInterval(it->second, disjointintervals[it->first]);
  }

  for (it = disjointintervals.begin(); it != disjointintervals.end(); it++) {
    const Key& key = it->first;
    // 根据shardID得到对应的BucketMap
    auto map = getShardMap(key.shardId);
    if (!map || key.key.length() > kMaxKeyLength) {
      break;
    }
    // 获取BucketMap的状态
    if (map->getState() != BucketMap::OWNED) {
      break;
    }
    auto row = map->get(key.key);
    if (!row.get()) {
      continue;
    }
    for (i = 0; i < it->second.size(); i++) {
      begin_time = it->second[i].first;
      end_time = it->second[i].second - 1;
      if (!row->second.lockDataPointsInTS(
              begin_time,
              end_time,
              shards_.getShardMap(0)->bucket(begin_time),
              shards_.getShardMap(0)->bucket(end_time),
              map->getStorage())) {
        break;
      }
    }

    // sections.emplace_back(begin_time, end_time);
  }
  if (it != disjointintervals.end()) {
    for (auto itrb = disjointintervals.begin(); itrb != it; itrb++) {
      unlockDataPointsInOp(itrb->first, itrb->second);
    }
    return false;
  }
  return true;
}

// written by yls
// 为给定时序的多个区间内数据解锁
bool BeringeiServiceHandler::unlockDataPointsInOp(
    const Key& key,
    std::vector<std::pair<int64_t, int64_t>>& intervals) {
  bool ret = true;
  // 根据shardID得到对应的BucketMap
  auto map = getShardMap(key.shardId);
  if (!map || key.key.length() > kMaxKeyLength) {
    return false;
  }
  // 获取BucketMap的状态
  if (map->getState() != BucketMap::OWNED) {
    return false;
  }
  auto row = map->get(key.key);
  if (!row.get()) {
    return true;
  }
  int64_t begin_time;
  int64_t end_time;
  for (int i = 0; i < intervals.size(); i++) {
    begin_time = intervals[i].first;
    end_time = intervals[i].second - 1;
    ret = ret &&
        row->second.unlockDataPointsInTS(
            begin_time,
            end_time,
            shards_.getShardMap(0)->bucket(begin_time),
            shards_.getShardMap(0)->bucket(end_time),
            map->getStorage());
  }

  return ret;
}

// written by yls
// 为事务中的每个操作访问的数据解锁
bool BeringeiServiceHandler::unlockDataPointsInOp(
    std::vector<TxRequestOp>& op) {
  std::unordered_map<Key, std::vector<std::pair<int64_t, int64_t>>, KeyHash>
      intervals;
  std::unordered_map<Key, std::vector<std::pair<int64_t, int64_t>>, KeyHash>
      disjointintervals;
  int i;
  bool ret = true;
  int64_t begin_time;
  int64_t end_time;

  for (i = 0; i < op.size(); i++) {
    if (op[i].get_opcode() == TxOpCode::INSERT)
      continue;
    if (op[i].get_opcode() == TxOpCode::SCAN) {
      begin_time = op[i].get_startTime();
      end_time = op[i].get_endTime();
    } else {
      begin_time = op[i].get_value().get_unixTime();
      end_time = begin_time;
    }
    intervals[op[i].get_key()].emplace_back(begin_time, end_time + 1);
  }

  auto it = intervals.begin();
  for (; it != intervals.end(); it++) {
    mergeInterval(it->second, disjointintervals[it->first]);
  }

  for (it = disjointintervals.begin(); it != disjointintervals.end(); it++) {
    ret = ret && unlockDataPointsInOp(it->first, it->second);
    // sections.emplace_back(begin_time, end_time);
  }
  return ret;
}

void BeringeiServiceHandler::checkpoint() {
  {
    std::unique_lock<std::mutex> lock(checkpointMutex_);
    while (!requireCheckpoint) {
      checkpointReadyCv_.wait(lock);
    }
  }

  int i;
  std::vector<std::shared_ptr<std::vector<TimeSeriesStream>>> copyOfTSStreams(shards_.getTotalNumShards());
  std::vector<std::thread> threads;

  LOG(INFO) << "start checkpoint! ";
  pthread_rwlock_wrlock(&insertTxRwlock_);
  pthread_rwlock_wrlock(&updateTxRwlock_);

  BucketMap* shard;
  for (i = 0; i < shards_.getTotalNumShards(); i++) {
    shard = shards_.getShardMap(i);
    auto logWriter = shard->getLogWriter();
    threads.emplace_back([&copyOfTSStreams, shard, i, logWriter]() {
      prctl(PR_SET_NAME, "copyStreams");
      logWriter->logCheckpoint(
        i,
        DataLogReader::kStartCheckpoint,
        time(nullptr),
        0,
        FLAGS_log_flag_checkpoint);
      shard->resetExtension();
      copyOfTSStreams[i] = shard->copyStreams();
      
    });
  }
  for (i = 0; i < shards_.getTotalNumShards(); i++) {
    threads[i].join();
  }
  pthread_rwlock_wrlock(&BucketedTimeSeries::bucketsRwlock_);
  pthread_rwlock_unlock(&insertTxRwlock_);
  threads.clear();
  for (i = 0; i < shards_.getTotalNumShards(); i++) {
    shard = shards_.getShardMap(i);
    threads.emplace_back([shard]() {
      prctl(PR_SET_NAME, "finalizeBuckets");
      shard->finalizeBuckets();
    });
  }
  for (i = 0; i < shards_.getTotalNumShards(); i++) {
    threads[i].join();
  }

  pthread_rwlock_unlock(&BucketedTimeSeries::bucketsRwlock_);
  pthread_rwlock_unlock(&updateTxRwlock_);
  LOG(INFO) << "end checkpoint! ";

  {
    std::unique_lock<std::mutex> lock(checkpointMutex_);
    requireCheckpoint = false;
    checkpointCompleteCv_.notify_all();
  }
  threads.clear();
  for (i = 0; i < shards_.getTotalNumShards(); i++) {
    shard = shards_.getShardMap(i);
    auto logWriter = shard->getLogWriter();
    threads.emplace_back([&copyOfTSStreams, shard, i, logWriter]() {
      prctl(PR_SET_NAME, "finalizeStreams");
      shard->finalizeStreams(*(copyOfTSStreams[i].get()));
      logWriter->logCheckpoint(
        i,
        DataLogReader::kStopCheckpoint,
        time(nullptr),
        0,
        FLAGS_log_flag_checkpoint);
    });
  }
  for (i = 0; i < shards_.getTotalNumShards(); i++) {
    threads[i].join();
  }

}

void BeringeiServiceHandler::getShardDataBucket(
    GetShardDataBucketResult& ret,
    int64_t beginTs,
    int64_t endTs,
    int64_t shardId,
    int32_t offset,
    int32_t limit) {
  beginTs = BucketUtils::floorTimestamp(beginTs, FLAGS_bucket_size, shardId);
  endTs = BucketUtils::floorTimestamp(endTs, FLAGS_bucket_size, shardId);

  LOG(INFO) << "Fetching data for shard " << shardId << " between time "
            << beginTs << " and " << endTs;

  Timer timer(true);

  ret.moreEntries = false;
  auto map = shards_.getShardMap(shardId);
  if (!map) {
    ret.status = StatusCode::RPC_FAIL;
    return;
  }
  auto state = map->getState();
  if (state != BucketMap::OWNED) {
    ret.status = state <= BucketMap::UNOWNED ? StatusCode::DONT_OWN_SHARD
                                             : StatusCode::SHARD_IN_PROGRESS;
    return;
  }

  // Annotated by gyy
  // // Don't allow data fetches until the bucket has been finalized.
  // if (map->bucket(endTs) > map->getLastFinalizedBucket()) {
  //   ret.status = StatusCode::BUCKET_NOT_FINALIZED;
  //   return;
  // }

  std::vector<BucketMap::Item> rows;
  ret.moreEntries = map->getSome(rows, offset, limit);
  auto storage = map->getStorage();

  ret.keys.reserve(rows.size());
  ret.data.reserve(rows.size());

  uint32_t begin = map->bucket(beginTs);
  uint32_t end = map->bucket(endTs);

  for (auto& row : rows) {
    if (row.get()) {
      std::vector<TimeSeriesBlock> blocks;
      row->second.get(beginTs, endTs, begin, end, blocks, storage);

      if (blocks.size() > 0) {
        ret.keys.push_back(row->first);
        ret.data.push_back(std::move(blocks));
        ret.recentRead.push_back(
            row->second.getQueriedBucketsAgo() <=
            map->buckets(kGorillaSecondsPerDay));
      }
    }
  }

  LOG(INFO) << "Data fetch for shard " << shardId << " complete in "
            << timer.get() << "us with " << ret.keys.size() << " keys returned";
}

void BeringeiServiceHandler::refreshShardConfig() {
  std::string hostName = NetworkUtils::getLocalHost();
  auto hostInfo = std::make_pair(hostName, port_);

  // ShardList will be populated by getShardsForHost.
  std::set<int64_t> shardList;
  configAdapter_->getShardsForHost(hostInfo, serviceName_, shardList);

  // We will addShard everything we should own and dropShard all other shards.
  // For anything we already own and should (or do not own and shouldn't), this
  // is a noop.
  shards_.setShards(shardList);
}

void BeringeiServiceHandler::purgeThread() {
  int numPurged = purgeTimeSeries(FLAGS_buckets);
  LOG(INFO) << "Purged " << numPurged << " time series.";
  GorillaStatsManager::addStatValue(kPurgedTimeSeries, numPurged);
}

void BeringeiServiceHandler::cleanThread() {
  Timer timer(true);
  LOG(INFO) << "Compressing key lists and deleting old block files";
  for (auto& bucketMap : shards_) {
    if (bucketMap->getState() != BucketMap::OWNED) {
      continue;
    }
    bucketMap->compactKeyList();
    bucketMap->deleteOldBlockFiles();
  }
  LOG(INFO) << "Done compressing key lists and deleting old block files";
  GorillaStatsManager::addStatValue(
      kMsPerKeyListCompact, timer.get() / kGorillaUsecPerMs);
}

BucketMap* BeringeiServiceHandler::getShardMap(int64_t shardId) {
  return shards_[shardId];
}

void BeringeiServiceHandler::getLastUpdateTimes(
    GetLastUpdateTimesResult& ret,
    std::unique_ptr<GetLastUpdateTimesRequest> req) {
  auto map = getShardMap(req->shardId);
  if (!map) {
    LOG(ERROR) << "Trying to get last update times for an invalid shard!";
    return;
  }

  if (map->getState() != BucketMap::OWNED) {
    LOG(ERROR) << "Trying to get last update times for an unowned shard!";
    return;
  }

  Timer timer(true);

  std::vector<BucketMap::Item> timeSeriesData;
  ret.moreResults = map->getSome(timeSeriesData, req->offset, req->limit);

  for (auto& timeSeries : timeSeriesData) {
    if (timeSeries.get()) {
      uint32_t lastUpdateTime =
          timeSeries->second.getLastUpdateTime(map->getStorage(), *map);
      if (lastUpdateTime >= req->minLastUpdateTime) {
        KeyUpdateTime key;
        key.key = timeSeries->first;

        key.categoryId = timeSeries->second.getCategory();
        key.updateTime = lastUpdateTime;

        uint8_t queriedBucketsAgo = timeSeries->second.getQueriedBucketsAgo();
        key.queriedRecently =
            queriedBucketsAgo <= map->buckets(kGorillaSecondsPerDay);

        ret.keys.push_back(std::move(key));
      }
    }
  }

  GorillaStatsManager::addStatValue(kUsPerGetLastUpdateTimes, timer.get());
}

int BeringeiServiceHandler::purgeTimeSeries(uint8_t numBuckets) {
  int purgedTimeSeries = 0;

  try {
    std::unordered_map<int32_t, int64_t> purgedTSPerCategory;
    for (auto& bucketMap : shards_) {
      if (bucketMap->getState() != BucketMap::OWNED) {
        continue;
      }

      std::vector<BucketMap::Item> timeSeriesData;
      bucketMap->getEverything(timeSeriesData);
      for (int i = 0; i < timeSeriesData.size(); i++) {
        if (timeSeriesData[i].get()) {
          uint16_t category = timeSeriesData[i]->second.getCategory();
          if (!timeSeriesData[i]->second.hasDataPoints(numBuckets)) {
            bucketMap->erase(i, timeSeriesData[i]);
            ++purgedTimeSeries;
            ++purgedTSPerCategory[category];
          }
        }
      }
    }

    for (auto item : purgedTSPerCategory) {
      GorillaStatsManager::setCounter(
          kPurgedTimeSeriesInCategoryPrefix + std::to_string(item.first),
          item.second);
    }
  } catch (std::exception& e) {
    LOG(ERROR) << e.what();
  }

  return purgedTimeSeries;
}

void BeringeiServiceHandler::finalizeBucketsThread() {
  // This is the last bucket that can be finalized at this
  // moment. It considers that timestamps can be late and adds
  // one minute buffer to allow the data to be processed.
  //
  // The same bucket is finalized multiple times on purpose to make
  // sure all the shard movements are caught.
  /*
  uint64_t timestamp = time(nullptr) - FLAGS_allowed_timestamp_behind -
      kGorillaSecondsPerMinute - BucketUtils::duration(1, FLAGS_bucket_size);
  bool behind = false;
  for (int i = 0; i < FLAGS_shards; i++) {
    uint32_t bucketToFinalize = shards_[i]->bucket(timestamp);
    if (shards_[i]->isBehind(bucketToFinalize)) {
      behind = true;
    }
  }

  if (behind) {
    GorillaStatsManager::addStatValue(kTooSlowToFinalizeBuckets);
    LOG(ERROR) << "Finalizing the previous buckets took too long!";
  }
  */

  finalizeBucket(time(nullptr));
}

void BeringeiServiceHandler::finalizeBucket(const uint64_t timestamp) {
  LOG(INFO) << "Finalizing buckets at time " << timestamp;

  // Put all the shards in the queue even if they are not owned
  // because they might be owned 5 minutes later.
  folly::MPMCQueue<uint32_t> queue(FLAGS_shards);
  for (int i = 0; i < FLAGS_shards; i++) {
    queue.write(i);
  }

  // Create a fixed number of threads and go through all the shards.
  std::vector<std::thread> threads;
  for (int i = 0; i < FLAGS_block_writer_threads; i++) {
    threads.emplace_back([&]() {
      while (true) {
        uint32_t shardId;
        if (!queue.read(shardId)) {
          break;
        }

        // uint32_t bucketToFinalize = shards_[shardId]->bucket(timestamp);
        Timer timer(true);

        // If the shard is not owned or there are no buckets to
        // finalized, this will return immediately with 0.
        shards_[shardId]->finalizeBuckets();

        GorillaStatsManager::addStatValueAggregated(
            kMsPerFinalizeShardBucket,
            timer.get() / kGorillaUsecPerMs,
            FLAGS_buckets);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}
} // namespace gorilla
} // namespace facebook

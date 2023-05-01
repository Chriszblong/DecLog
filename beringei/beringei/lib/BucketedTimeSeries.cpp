/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <atomic>

#include "BitUtil.h"
#include "BucketMap.h"
#include "BucketedTimeSeries.h"

DEFINE_int32(
    mintimestampdelta,
    -600,
    "Values coming in faster than this are considered spam");

DEFINE_int32(
    num_of_operations_in_parallel,
    20,
    "# operations accessing different data points can run in parallel");

DEFINE_int32(extention_len, 10, "Number of bits in extention(LSN).");

DECLARE_int64(data_point_nonexistent);
// DECLARE_int64(tombstone_setted);
DECLARE_int64(data_point_in_updatedDataMap);

namespace facebook {
namespace gorilla {

DECLARE_int32(unix_time_bits);

static const uint16_t kDefaultCategory = 0;
pthread_rwlock_t BucketedTimeSeries::bucketsRwlock_ = PTHREAD_RWLOCK_INITIALIZER;

BucketedTimeSeries::BucketedTimeSeries() {
  updateDataPointLocks_.resize(FLAGS_num_of_operations_in_parallel);
}

BucketedTimeSeries::~BucketedTimeSeries() {}

void BucketedTimeSeries::reset(uint8_t n) {
  queriedBucketsAgo_ = std::numeric_limits<uint8_t>::max();
  // lock_.init();
  current_ = 0;
  blocks_.reset(new BucketStorage::BucketStorageId[n]);

  for (int i = 0; i < n; i++) {
    blocks_[i] = BucketStorage::kInvalidId;
  }
  count_ = 0;
  stream_.reset();
  stream_.extraData = kDefaultCategory;
}

bool BucketedTimeSeries::put(
    uint32_t i,
    const TimeValuePair& value,
    BucketStorage* storage,
    uint32_t timeSeriesId,
    uint16_t* category) {
  // folly::MSLGuard guard(lock_);
  // Here use readGuard to avoid update operations block insertion.
  folly::RWSpinLock::ReadHolder readGuard(lock_);
  // folly::RWSpinLock::WriteHolder writeGuard(lock_);
  if (i < current_) {
    return false;
  }

  if (i != current_) {
    // Release the lock_;
    readGuard.reset();
    // Get the writeGuard
    folly::RWSpinLock::WriteHolder writeGuard(lock_);
    pthread_rwlock_rdlock(&bucketsRwlock_);
    open(i, storage, timeSeriesId);
    pthread_rwlock_unlock(&bucketsRwlock_);
  }
  readGuard.reset(&lock_);

  if (!stream_.append(value, FLAGS_mintimestampdelta)) {
    return false;
  }

  if (category) {
    stream_.extraData = *category;
  }

  count_++;
  return true;
}

void BucketedTimeSeries::get(
    int64_t timeBegin,
    int64_t timeEnd,
    uint32_t begin,
    uint32_t end,
    std::vector<TimeSeriesBlock>& out,
    BucketStorage* storage) {
  uint8_t n = storage->numBuckets();
  out.reserve(out.size() + std::min<uint32_t>(n + 1, end - begin + 1) + 1);

  // folly::MSLGuard guard(lock_);
  folly::RWSpinLock::ReadHolder readGuard(lock_);
  bool getCurrent = begin <= current_ && end >= current_;

  end = std::min(end, current_ >= 1 ? current_ - 1 : 0);
  begin = std::max(begin, current_ >= n ? current_ - n : 0);

  // The first block is the updateDataMap_;
  TimeSeriesBlock mapBlock;

  // The map is not compressed for now.
  mapBlock.data.reserve(updatedDataMap_.size() * 8 * 2);

  // Read data point between timeBegin and timeEnd from updatedDataMap_.
  // Convert the updatedDataMap_ into data block.
  folly::RWSpinLock::ReadHolder readMapGuard(updatedDataMapLock_);

  auto itLower = updatedDataMap_.lower_bound(timeBegin);
  auto itUpper = updatedDataMap_.upper_bound(timeEnd);

  // Record the number of items.
  uint16_t itemCount = 0;

  // Record the bits.
  folly::fbstring tempStr;
  uint32_t numBits = 0;

  std::size_t extention_len = (FLAGS_extention_len >> 3) + 1;
  for (auto it = itLower; it != itUpper; ++it) {
    // The size of value smaller than extention_len indicates that
    // the data point has been removed.
    if (it->second.size() > extention_len) {
      // Append timestamp
      BitUtil::addValueToBitString(it->first, 64, tempStr, numBits);
      // BitUtil::addValueToBitString(*(uint64_t*)(&(it->first)), 64, tempStr,
      // numBits);

      // LOG(INFO) << "Timestamp is: " << it->first << std::endl;
      // Append value. The value length is 64 bit, so read 8 chars.
      // And the remaining data is LSN which is not needed for the query.
      tempStr.append(it->second.substr(0, 8).c_str(), 8);
      // uint64_t bitPos = 0;
      // uint64_t value = BitUtil::readValueFromBitString(it->second.c_str(),
      // bitPos, 64); double lastValue = *(double*)(&value); LOG(INFO) << "Value
      // is: " << lastValue << std::endl;
      ++itemCount;
    }
  }

  // Push the mapBlock into out.
  mapBlock.count = itemCount;
  // LOG(INFO) << "itemCount is: " << mapBlock.count << std::endl;
  mapBlock.data.assign(tempStr.c_str(), tempStr.length());
  out.push_back(std::move(mapBlock));

  for (int i = begin; i <= end; i++) {
    TimeSeriesBlock outBlock;
    uint16_t count;

    BucketStorage::FetchStatus status =
        storage->fetch(i, blocks_[i % n], outBlock.data, count);
    if (status == BucketStorage::FetchStatus::SUCCESS) {
      outBlock.count = count;
      out.push_back(std::move(outBlock));
    }
  }

  if (getCurrent) {
    out.emplace_back();
    out.back().count = count_;
    stream_.readData(out.back().data);
  }
}

void BucketedTimeSeries::getExtension(
    int64_t timeBegin,
    int64_t timeEnd,
    std::vector<std::pair<int64_t, uint64_t>>& out) {
  // Read data point between timeBegin and timeEnd from updatedDataMap_.
  // Convert the updatedDataMap_ into data block.
  folly::RWSpinLock::ReadHolder readMapGuard(updatedDataMapLock_);

  auto itLower = updatedDataMap_.lower_bound(timeBegin);
  auto itUpper = updatedDataMap_.upper_bound(timeEnd);

  int64_t timestamp;
  uint64_t extension;
  uint64_t bitPos;

  std::size_t extention_len = FLAGS_extention_len >> 3 + 1;
  for (auto it = itLower; it != itUpper; ++it) {
    // The size of value smaller than extention_len indicates that
    // the data point has been removed.
    timestamp = it->first;
    if (it->second.size() > extention_len) {
      bitPos = 64;
      extension = BitUtil::readValueFromBitString(
          it->second.c_str(), bitPos, FLAGS_extention_len);
    } else {
      bitPos = 0;
      extension = BitUtil::readValueFromBitString(
          it->second.c_str(), bitPos, FLAGS_extention_len);
    }
    out.emplace_back(timestamp, extension);
  }
}

// written by yls
// 给指定时间序列中指定范围数据点上锁
bool BucketedTimeSeries::lockDataPointsInTS(
    int64_t begin,
    int64_t end,
    uint32_t bucketBegin,
    uint32_t bucketEnd,
    BucketStorage* storage) {
  uint8_t n = storage->numBuckets();
  int i;

  folly::RWSpinLock::ReadHolder readGuard(lock_);
  // folly::MSLGuard guard(lock_);
  bool lockCurrent = bucketBegin <= current_ && bucketEnd >= current_;

  bucketEnd = std::min(
      bucketEnd, current_ >= 1 ? current_ - 1 : 0); // 读到current_的上一个位置
  bucketBegin = std::max(bucketBegin, current_ >= n ? current_ - n : 0);

  folly::MSLGuard guard(
      updateDataPointLocks_[begin % FLAGS_num_of_operations_in_parallel]);
  for (i = bucketBegin; i <= bucketEnd; i++) {
    if (!storage->lockDataPointInBucket(begin, end, i, blocks_[i % n]))
      break;
  }
  --i;
  if (i < bucketEnd) {
    while (i >= bucketBegin) {
      storage->unlockDataPointInBucket(begin, end, i, blocks_[i % n]);
      --i;
    }
    return false;
  }

  if (lockCurrent) {
    if (!TimeSeriesStream::lockInExtension(
            stream_, (uint16_t)stream_.size(), (uint16_t)count_, begin, end)) {
      for (i = bucketEnd; i >= bucketBegin; i--) {
        storage->unlockDataPointInBucket(begin, end, i, blocks_[i % n]);
      }
      return false;
    }
  }

  return true;
}

// written by yls
// 给指定时间序列中指定范围数据点解锁
bool BucketedTimeSeries::unlockDataPointsInTS(
    int64_t begin,
    int64_t end,
    uint32_t bucketBegin,
    uint32_t bucketEnd,
    BucketStorage* storage) {
  uint8_t n = storage->numBuckets();
  int i;
  bool ret = true;

  folly::RWSpinLock::ReadHolder readGuard(lock_);
  bool unlockCurrent = bucketBegin <= current_ && bucketEnd >= current_;

  bucketEnd = std::min(
      bucketEnd, current_ >= 1 ? current_ - 1 : 0); // 读到current_的上一个位置
  bucketBegin = std::max(bucketBegin, current_ >= n ? current_ - n : 0);

  if (unlockCurrent) {
    ret = ret &&
        TimeSeriesStream::unlockInExtension(
              stream_, (uint16_t)stream_.size(), (uint16_t)count_, begin, end);
  }

  for (i = bucketEnd; i >= bucketBegin; i--) {
    ret =
        ret && storage->unlockDataPointInBucket(begin, end, i, blocks_[i % n]);
  }

  return ret;
}

char* BucketedTimeSeries::findDataPoint(
    BucketStorage* storage,
    const int& b,
    const TimeValuePair& value,
    int64_t& tombstoneBitID,
    int64_t& targetTimestamp) {
  uint8_t n = storage->numBuckets();

  // folly::RWSpinLock::ReadHolder readGuard(lock_);

  tombstoneBitID = FLAGS_data_point_nonexistent;

  char* targetBlock = NULL;

  // Find the data point in current stream.
  if (b == current_) {
    // targetBlock = stream_.getDataPtr();
    tombstoneBitID = TimeSeriesStream::findDataPointInStream(
        stream_.getDataPtr(), value, count_, targetTimestamp);
    // LOG(INFO) << "Current!!!!!!!!!!!!" << std::endl;
  }
  // Find the data point in storage.
  else {
    targetBlock = storage->findDataPointInBucket(
        value, b, blocks_[b % n], tombstoneBitID, targetTimestamp);
  }

  // if (tombstoneBitID == FLAGS_data_point_nonexistent) {
  //   LOG(INFO) << "The tombstoneBitID is " << tombstoneBitID << std::endl;
  // }

  if (tombstoneBitID != FLAGS_data_point_nonexistent) {
    uint64_t bitPos = tombstoneBitID;
    uint64_t tombstone;
    if (b == current_) {
      tombstone =
          BitUtil::readValueFromBitString(stream_.data_.c_str(), bitPos, 1);
    } else {
      tombstone = BitUtil::readValueFromBitString(targetBlock, bitPos, 1);
    }

    // The tombstone of the data point is setted,
    // the data point should be queried in the updatedDataMap_.
    if (tombstone) {
      folly::RWSpinLock::ReadHolder readMapGuard(updatedDataMapLock_);
      auto iter = updatedDataMap_.find(value.unixTime);

      // The data point is in the updatedDataMap_;
      if (iter != updatedDataMap_.end()) {
        // The LSN is recorded behind the value.
        // So the begin bit position is 64.
        uint64_t curBitPos = 64;

        // uint64_t oriValue =
        // BitUtil::readValueFromBitString(iter->second.c_str(), curBitPos, 64);
        // LOG(INFO) << "The original char is " << oriValue << std::endl;
        uint64_t LSN = BitUtil::readValueFromBitString(
            iter->second.c_str(), curBitPos, FLAGS_extention_len);

        // LOG(INFO) << "The original LSN is " << LSN << std::endl;

        // Merge the extention with the timestamp.
        targetTimestamp = LSN << FLAGS_unix_time_bits | value.unixTime;

      }
      // The data point has been removed by other operation.
      // Nothing to do here, because the function of confirmDataPointExits()
      // will find find the data point again.
      else {
        // LOG(INFO) << "The data point is missing??????????" << std::endl;
        // tombstoneBitID = tombstoneBitID;
        tombstoneBitID = FLAGS_data_point_nonexistent;
      }
    }
  }
  // The data point may be inserted before the current active bucket.
  // Find the data point from updatedDataMap;
  else {
    folly::RWSpinLock::ReadHolder readMapGuard(updatedDataMapLock_);
    auto iter = updatedDataMap_.find(value.unixTime);
    // The data point is in the updatedDataMap_;
    if (iter != updatedDataMap_.end()) {
      tombstoneBitID = FLAGS_data_point_in_updatedDataMap;
    }
  }
  return targetBlock;
}

void BucketedTimeSeries::setTombstone(
    char* targetBlock,
    const int& b,
    int64_t tombstoneBitID,
    const TimeValuePair& value,
    const uint64_t& LSN,
    const bool& isDeletion) {
  // folly::RWSpinLock::ReadHolder readGuard(lock_);

  uint64_t bitPos = tombstoneBitID;
  uint64_t tombstone;
  uint64_t bytePos;
  uint64_t bitRemain;
  bytePos = tombstoneBitID >> 3;
  bitRemain = tombstoneBitID & 0x7;
  // First Read the tombstoneBit, set the tombstone if it is not setted.
  if (b == current_) {
    // Read the tombstone
    tombstone =
        BitUtil::readValueFromBitString(stream_.data_.c_str(), bitPos, 1);
    if (!tombstone) {
      stream_.data_[bytePos] = stream_.data_[bytePos] | (0x80 >> bitRemain);
    }
  } else {
    // Read the tombstone
    tombstone = BitUtil::readValueFromBitString(targetBlock, bitPos, 1);
    if (!tombstone) {
      targetBlock[bytePos] = targetBlock[bytePos] | (0x80 >> bitRemain);
    }
  }

  // If the caller is data point deletion.
  // Find the data point from updatedDataMap_.
  // If the data point exists, remove the value bits to indicate
  // that the data point has been removed.
  // The LSN must be remained for the following operation which inserts
  // a data point whose key is the same with the former deleted data point.
  if (isDeletion) {
    folly::RWSpinLock::ReadHolder readMapGuard(updatedDataMapLock_);
    auto iter = updatedDataMap_.find(value.unixTime);

    // The data point cannot erase right now,
    // because other operation may insert the same data point later.
    // And the LSN should be calculated by the original value.
    // We add an extra bit into the value to indicate that the data
    // point has been deleted.
    if (iter != updatedDataMap_.end()) {
      readMapGuard.reset();
      folly::RWSpinLock::WriteHolder writeMapGuard(updatedDataMapLock_);
      // Remove the value fconfirmDatarom iter->second to indicate the data
      // point has been deleted. The LSN can not been removed, because the new
      // LSN must be calculated by the original one whenever another operation
      // inserts a data point whose key is the same with the original one.
      iter->second = iter->second.substr(8);
    }
  }
}

bool BucketedTimeSeries::confirmDataPointExits(
    char* targetBlock,
    const int& b,
    int64_t& tombstoneBitID,
    const TimeValuePair& value,
    int64_t& targetTimestamp) {
  // folly::RWSpinLock::ReadHolder readGuard(lock_);

  // Read the tombstone bit.
  uint64_t bitPos = tombstoneBitID;

  uint64_t tombstoneBit;

  if (b == current_) {
    tombstoneBit =
        BitUtil::readValueFromBitString(stream_.data_.c_str(), bitPos, 1);
    // LOG(INFO) << "Current!!!!!!!!!!!!" << std::endl;
  } else {
    tombstoneBit = BitUtil::readValueFromBitString(targetBlock, bitPos, 1);
  }

  // The tombstoneBit has been setted by other thread.
  // Find the data point from the updateDataMap.
  if (tombstoneBit) {
    folly::RWSpinLock::ReadHolder readMapGuard(updatedDataMapLock_);
    auto iter = updatedDataMap_.find(value.unixTime);

    uint64_t LSN;
    uint64_t curBitPos;
    // The LSN is recorded behind the value.
    // So the LSN bit position is 64.
    if (iter->second.size() > 8) {
      curBitPos = 64;
      LSN = BitUtil::readValueFromBitString(
          iter->second.c_str(), curBitPos, FLAGS_extention_len);
    }
    // The value has been deleted by other data point deletion operation.
    // The value just contains the LSN.
    else {
      curBitPos = 0;
      LSN = BitUtil::readValueFromBitString(
          iter->second.c_str(), curBitPos, FLAGS_extention_len);
    }

    // Merge the extention with the timestamp.
    targetTimestamp = LSN << FLAGS_unix_time_bits | value.unixTime;

    // LOG(INFO) << "The LSN in updatedDataMap is " << LSN << std::endl;
    // LOG(INFO) << "The target timestamp is " << value.unixTime << std::endl;
    // LOG(INFO) << "The tombstoneBit is " << tombstoneBit << std::endl;
    // LOG(INFO) << "The timestamp with extention is " << targetTimestamp <<
    // std::endl; LOG(INFO) << "The target value is " << iter->second <<
    // std::endl;

    // The data point has been deleted by other thread.
    return iter != updatedDataMap_.end();
  }
  // The data point exists in the stream.
  else {
    return true;
  }
}

void BucketedTimeSeries::insertIntoDataMap(
    const TimeValuePair& value,
    const uint64_t& LSN) {
  // uint64_t firstValue = stream_.getFirstValue();
  folly::fbstring valueStr;
  uint32_t numBits = 0;

  // Append value.
  // It is not often for update in time series databases.
  // So the value is not compressed for now.
  BitUtil::addValueToBitString(
      *(uint64_t*)&(value.value), 64, valueStr, numBits);

  // Append LSN.
  BitUtil::addValueToBitString(LSN, FLAGS_extention_len, valueStr, numBits);

  folly::RWSpinLock::WriteHolder writeMapGuard(updatedDataMapLock_);
  updatedDataMap_.insert(std::make_pair(value.unixTime, valueStr));
}

void BucketedTimeSeries::updateDataPointInDataMap(
    const TimeValuePair& value,
    const uint64_t& LSN) {
  // uint64_t firstValue = stream_.getFirstValue();
  folly::fbstring valueStr;
  uint32_t numBits = 0;

  // Append value.
  // It is not often for update in time series databases.
  // So the value is not compressed for now.
  BitUtil::addValueToBitString(
      *(uint64_t*)(&value.value), 64, valueStr, numBits);

  // Append LSN.
  BitUtil::addValueToBitString(LSN, FLAGS_extention_len, valueStr, numBits);

  folly::RWSpinLock::WriteHolder writeMapGuard(updatedDataMapLock_);
  updatedDataMap_[value.unixTime] = valueStr;
}

void BucketedTimeSeries::deleteDataPointInDataMap(const TimeValuePair& value) {
  folly::RWSpinLock::WriteHolder writeMapGuard(updatedDataMapLock_);
  // Remove the value from iter->second to indicate the data point
  // has been deleted. The LSN can not been removed, because the new LSN
  // must be calculated by the original one whenever another operation
  // inserts a data point whose key is the same with the original one.
  updatedDataMap_[value.unixTime] = updatedDataMap_[value.unixTime].substr(8);
}

int64_t BucketedTimeSeries::readTimestampFromDataMap(
    const TimeValuePair& value) {
  folly::RWSpinLock::ReadHolder readMapGuard(updatedDataMapLock_);
  auto iter = updatedDataMap_.find(value.unixTime);

  uint64_t LSN;
  uint64_t curBitPos;
  // The LSN is recorded behind the value.
  // So the begin bit position is 64.
  if (iter->second.size() > (FLAGS_extention_len >> 3 + 1)) {
    curBitPos = 64;
    LSN = BitUtil::readValueFromBitString(
        iter->second.c_str(), curBitPos, FLAGS_extention_len);
  }
  // The value has been deleted by other data point deletion operation.
  else {
    curBitPos = 0;
    LSN = BitUtil::readValueFromBitString(
        iter->second.c_str(), curBitPos, FLAGS_extention_len);
  }

  // LOG(INFO) << "The LSN is " << LSN << std::endl;
  // Merge the extention with the timestamp.
  return LSN << FLAGS_unix_time_bits | value.unixTime;
}

void BucketedTimeSeries::updateDataPoint(
    const uint64_t& LSN,
    const TimeValuePair& value) {
  // uint64_t firstValue = stream_.getFirstValue();
  folly::fbstring valueStr;
  uint32_t numBits = 0;

  // Append value.
  // It is not often for update in time series databases.
  // So the value is not compressed for now.
  BitUtil::addValueToBitString(
      *(uint64_t*)(&value.value), 64, valueStr, numBits);

  // LOG(INFO) << "The updated value is: " << value.value << std::endl;

  // Append LSN.
  BitUtil::addValueToBitString(LSN, FLAGS_extention_len, valueStr, numBits);
  // LOG(INFO) << "The updated LSN is: " << LSN << std::endl;

  // uint64_t curBitPos = 0;
  // uint64_t curValue = BitUtil::readValueFromBitString(valueStr.c_str(),
  // curBitPos, 64); LOG(INFO) << "The updated value is: " << curValue <<
  // std::endl; uint64_t curLSN =
  // BitUtil::readValueFromBitString(valueStr.c_str(), curBitPos,
  // FLAGS_extention_len); LOG(INFO) << "The updated LSN is: " << curLSN <<
  // std::endl;

  {
    folly::RWSpinLock::WriteHolder writeMapGuard(updatedDataMapLock_);
    // LOG(INFO) << "The timestamp of the updated data point is " <<
    // value.unixTime << std::endl; LOG(INFO) << "The LSN of the updated data
    // point is " << LSN << std::endl; LOG(INFO) << "The value of the updated
    // data point is " << value.value << std::endl;

    auto iter = updatedDataMap_.find(value.unixTime);

    // The data point is in the updatedDataMap_;
    if (iter != updatedDataMap_.end()) {
      updatedDataMap_[value.unixTime] = valueStr;
    } else {
      updatedDataMap_.insert(std::make_pair(value.unixTime, valueStr));
    }
    // updatedDataMap_.insert(std::make_pair(value.unixTime, valueStr));
  }
}

void BucketedTimeSeries::lockDataPoint(
    char* targetBlock,
    const int& b,
    int64_t tombstoneBitID,
    const TimeValuePair& value) {
  // folly::RWSpinLock::ReadHolder readGuard(lock_);
  uint64_t lockBitID = tombstoneBitID + 1;

  // LOG(INFO) << "The tombstoneBit is  " << tombstoneBitID << std::endl;

  // Get the byteID of lock bit located.
  uint64_t bytePos;
  uint64_t bitRemain;
  bytePos = lockBitID >> 3;
  bitRemain = lockBitID & 0x7;

  // Get the lock.
  int curLock = value.unixTime % FLAGS_num_of_operations_in_parallel;
  folly::MSLGuard guard(updateDataPointLocks_[curLock]);

  if (b == current_) {
    // Wait for other operations finished.
    while (
        BitUtil::readValueFromBitString(stream_.data_.c_str(), lockBitID, 1)) {
      --lockBitID;
    }
    stream_.data_[bytePos] = stream_.data_[bytePos] | (0x80 >> bitRemain);
  } else {
    // Wait for other operations finished.
    while (BitUtil::readValueFromBitString(targetBlock, lockBitID, 1)) {
      --lockBitID;
    }

    // LOG(INFO) << "The target block address is  " << targetBlock << std::endl;

    // LOG(INFO) << "The lockbit value is  " <<
    // BitUtil::readValueFromBitString(targetBlock, lockBitID, 1) << std::endl;
    // --lockBitID;

    // LOG(INFO) << "The bytePos is  " << bytePos << std::endl;
    // LOG(INFO) << "The bitRemain is  " << bitRemain << std::endl;
    // Set the lock bit.
    // LOG(INFO) << "The value before the lockbit is  " << (targetBlock[bytePos
    // - 1]) << std::endl; LOG(INFO) << "The lockbit value is  " <<
    // targetBlock[bytePos] << std::endl;
    targetBlock[bytePos] = targetBlock[bytePos] | (0x80 >> bitRemain);
  }
}

void BucketedTimeSeries::unlockDataPoint(
    char* targetBlock,
    const int& b,
    int64_t tombstoneBitID,
    const TimeValuePair& value) {
  // folly::RWSpinLock::ReadHolder readGuard(lock_);

  uint64_t lockBitID = tombstoneBitID + 1;

  // Get the byteID of lock bit located.
  uint64_t bytePos;
  uint64_t bitRemain;
  bytePos = lockBitID >> 3;
  bitRemain = lockBitID & 0x7;

  // Get the lock
  // int curLock = value.unixTime % FLAGS_num_of_operations_in_parallel;
  // folly::MSLGuard guard(updateDataPointLocks_[curLock]);

  // The MSLock is not needed here, because no other thread can set the lock
  // bit.
  if (b == current_) {
    // Unset the lock bit.
    stream_.data_[bytePos] = stream_.data_[bytePos] & ~(0x80 >> bitRemain);
  } else {
    // Unset the lock bit.
    targetBlock[bytePos] = targetBlock[bytePos] & ~(0x80 >> bitRemain);
  }
  // LOG(INFO) << "The lock has been unset :" << value.unixTime << std::endl;
}

void BucketedTimeSeries::setCurrentBucket(
    uint32_t currentBucket,
    BucketStorage* storage,
    uint32_t timeSeriesId) {
  uint32_t targetBucket;
  int t;
  // folly::MSLGuard guard(lock_);
  t = storage->getNewestPosition() - storage->numBuckets();
  if(t < currentBucket)
    targetBucket = t;
  else
    targetBucket = currentBucket;
  while (current_ <= targetBucket) {
    // Reset the block we're about to replace.
    auto& block = blocks_[current_ % storage->numBuckets()];

    if (count_ > 0) {
      // Copy out the active data.
      // The extension of timestamp should be removed before stored.
      LOG(ERROR) << "Trying to write data to an expired bucket";
    }
    block = BucketStorage::kInvalidId;

    // Prepare for writes.
    count_ = 0;
    stream_.reset();
    current_++;

    if (queriedBucketsAgo_ < std::numeric_limits<uint8_t>::max()) {
      queriedBucketsAgo_++;
    }
  }
  if (current_ < currentBucket) {
    folly::RWSpinLock::WriteHolder writeGuard(lock_);
    open(currentBucket, storage, timeSeriesId);
  }
}

void BucketedTimeSeries::open(
    uint32_t next,
    BucketStorage* storage,
    uint32_t timeSeriesId) {
  if (current_ == 0) {
    // Skip directly to the new value.
    current_ = next;
    return;
  }

  // Wipe all the blocks in between.
  while (current_ != next) {
    // Reset the block we're about to replace.
    auto& block = blocks_[current_ % storage->numBuckets()];

    if (count_ > 0) {
      // Copy out the active data.
      // The extension of timestamp should be removed before stored.
      block = storage->store(
          current_, stream_.getDataPtr(), stream_.size(), count_, timeSeriesId);
    } else {
      block = BucketStorage::kInvalidId;
    }

    // Prepare for writes.
    count_ = 0;
    stream_.reset();
    current_++;

    if (queriedBucketsAgo_ < std::numeric_limits<uint8_t>::max()) {
      queriedBucketsAgo_++;
    }
  }
}

void BucketedTimeSeries::setQueried() {
  queriedBucketsAgo_ = 0;
}

void BucketedTimeSeries::setDataBlock(
    uint32_t position,
    uint8_t numBuckets,
    BucketStorage::BucketStorageId id) {
  // folly::MSLGuard guard(lock_);
  folly::RWSpinLock::WriteHolder writeGuard(lock_);

  // Needed for time series that receive data very rarely.
  if (position >= current_) {
    current_ = position + 1;
    count_ = 0;
    stream_.reset();
  }

  blocks_[position % numBuckets] = id;
}

bool BucketedTimeSeries::hasDataPoints(uint8_t numBuckets) {
  // folly::MSLGuard guard(lock_);
  folly::RWSpinLock::ReadHolder readGuard(lock_);
  if (count_ > 0) {
    return true;
  }

  for (int i = 0; i < numBuckets; i++) {
    if (blocks_[i] != BucketStorage::kInvalidId) {
      return true;
    }
  }

  return false;
}

uint16_t BucketedTimeSeries::getCategory() const {
  // folly::MSLGuard guard(lock_);
  folly::RWSpinLock::ReadHolder readGuard(lock_);
  return stream_.extraData;
}

void BucketedTimeSeries::setCategory(uint16_t category) {
  // folly::MSLGuard guard(lock_);
  folly::RWSpinLock::WriteHolder writeGuard(lock_);
  stream_.extraData = category;
}

uint32_t BucketedTimeSeries::getLastUpdateTime(
    BucketStorage* storage,
    const BucketMap& map) {
  // folly::MSLGuard guard(lock_);
  folly::RWSpinLock::ReadHolder readGuard(lock_);
  uint32_t lastUpdateTime = stream_.getPreviousTimeStamp();
  if (lastUpdateTime != 0) {
    return lastUpdateTime;
  }

  // Nothing in the stream, find the latest block that has data and
  // return the end time of that block. The return value will just be
  // an estimate.
  for (int i = 0; i < storage->numBuckets(); i++) {
    int position = (int)current_ - 1 - i;
    if (position < 0) {
      break;
    }

    if (blocks_[position % storage->numBuckets()] !=
        BucketStorage::kInvalidId) {
      return map.timestamp(position + 1);
    }
  }

  return 0;
}
} // namespace gorilla
} // namespace facebook

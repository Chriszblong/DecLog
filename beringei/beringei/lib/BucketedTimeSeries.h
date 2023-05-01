/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <string>
#include <vector>

#include <folly/FBString.h>
#include <folly/SmallLocks.h>
#include "BucketStorage.h"
#include "TimeSeriesStream.h"
#include "BucketLogWriter.h"
#include "beringei/if/gen-cpp2/beringei_data_types.h"

namespace facebook {
namespace gorilla {

class BucketMap;
// Holds a rolling window of TimeSeries data.
class BucketedTimeSeries {
 public:
  BucketedTimeSeries();
  ~BucketedTimeSeries();

  // Initialize a BucketedTimeSeries with n historical buckets and
  // one active bucket.
  // Not thread-safe.
  void reset(uint8_t n);

  // Add a data point to the given bucket. Returns true if data was
  // added, false if it was dropped. If category pointer is defined,
  // sets the category.
  bool put(
      uint32_t i,
      const TimeValuePair& value,
      BucketStorage* storage,
      uint32_t timeSeriesId,
      uint16_t* category);

  // Read out buckets.
  // The first block in out is updatedDataMap.
  typedef std::vector<TimeSeriesBlock> Output;
  void get(int64_t timeBegin, int64_t timeEnd, uint32_t begin, uint32_t end, Output& out, BucketStorage* storage);


  //written by yls
  //读取扩展时间戳
  void getExtension(int64_t timeBegin, int64_t timeEnd, std::vector<std::pair<int64_t,uint64_t>>& out);

  //written by yls
  //给指定时间序列中指定范围数据点上锁
  bool lockDataPointsInTS(int64_t begin, int64_t end, uint32_t bucketBegin, uint32_t bucketEnd, BucketStorage* storage);

  //written by yls
  //给指定时间序列中指定范围数据点解锁
  bool unlockDataPointsInTS(int64_t begin, int64_t end, uint32_t bucketBegin, uint32_t bucketEnd, BucketStorage* storage);


  // Find the data point from bucket b.
  // If the data point exists, return the ID of lock bit, 
  // and the targetBlock points to the point locates.
  // If the data point does not exist, return -1.
  // The targetTimestamp is the data point timestamp.
  char* findDataPoint(
    BucketStorage* storage, 
    const int& b, 
    const TimeValuePair& value,
    int64_t& tombstoneBitID,
    int64_t& targetTimestamp);

  // Set the tombstone in targetBlock located at tombstoneBitID for the given data point.
  // LSN and isDeletion is needed for data point deletion.
  // If isDeletion is setted, remove the data point from updatedDataMap_ if it exists.
  void setTombstone(
    char* targetBlock, 
    const int& b,
    int64_t tombstoneBitID, 
    const TimeValuePair& value,
    const uint64_t& LSN = 0,
    const bool& isDeletion = 0);

  // Confirm the data point by tombstoneBitID and updateDataMap.
  // If exists, return true, else, return false.
  bool confirmDataPointExits(
    char* targetBlock, 
    const int& b,
    int64_t& tombstoneBitID, 
    const TimeValuePair& value,
    int64_t& targetTimestamp);

  // Update the data point if it is in updatedDataMap,
      // otherwise, insert the data point into updatedDataMap.
  void updateDataPoint(const uint64_t& LSN, const TimeValuePair& value);

  // Lock the data point and the lock bit in targetBlock is tombstoneBitID + 1.
  void lockDataPoint(char* targetBlock, const int& b, int64_t tombstoneBitID, const TimeValuePair& value);

  // Unlock the data point and the lock bit in targetBlock is tombstoneBitID + 1.
  void unlockDataPoint(char* targetBlock, const int& b, int64_t tombstoneBitID, const TimeValuePair& value);

  // Insert a data point to updatedDataMap
  // This function is only used when putting a data point into former buckets.
  void insertIntoDataMap(const TimeValuePair& value, const uint64_t& LSN);

  // Update a data point in updatedDataMap
  // This function is only used when the data point 
  // just exists in the updatedDataMap_.
  void updateDataPointInDataMap(const TimeValuePair& value, const uint64_t& LSN);

  // Delete a data point in updatedDataMap
  // This function is only used when the data point 
  // just exists in the updatedDataMap_.
  void deleteDataPointInDataMap(const TimeValuePair& value);

  // Get the Read lock of lock_.
  folly::RWSpinLock&  getStreamLock() {
    return lock_;
  }

  // Read the timestamp from updatedDataMap
  // This function is only used when the data point 
  // just exists in the updatedDataMap_.
  int64_t readTimestampFromDataMap(const TimeValuePair& value);

  // Returns a tuple representing:
  //   1) the number of points in the active stream.
  //   2) the number of bytes used by the stream.
  //   3) the number of bytes allocated for the stream.
  std::tuple<uint32_t, uint32_t, uint32_t> getActiveTimeSeriesStreamInfo() {
    folly::RWSpinLock::ReadHolder readGuard(lock_);
    // folly::MSLGuard guard(lock_);
    return std::make_tuple(count_, stream_.size(), stream_.capacity());
  }

  // Returns how many buckets ago this value was queried.
  // Will return 255 if it has never been queried.
  uint8_t getQueriedBucketsAgo() {
    return queriedBucketsAgo_;
  }

  // Sets that this time series was just queried.
  void setQueried();

  void setDataBlock(
      uint32_t position,
      uint8_t numBuckets,
      BucketStorage::BucketStorageId id);

  // Sets the current bucket. Flushes data from the previous bucket to
  // BucketStorage. No-op if this time series is already at
  // currentBucket.
  void setCurrentBucket(
      uint32_t currentBucket,
      BucketStorage* storage,
      uint32_t timeSeriesId);

  // Returns true if there are data points for this time series.
  bool hasDataPoints(uint8_t numBuckets);

  // Returns the ODS category associated with this time series.
  uint16_t getCategory() const;

  // Sets the ODS category for this time series.
  void setCategory(uint16_t category);

  uint32_t getLastUpdateTime(BucketStorage* storage, const BucketMap& map);

  // Get the currently active bucket.
  uint32_t getCurrentActiveBucket() {
    return current_;
  }

  // Get the current stream of data..
  TimeSeriesStream& getStream() {
    return stream_;
  }

  // Get the updatedDataMap_
  std::map<int64_t, folly::fbstring>& getUpdatedDataMap(){
    return updatedDataMap_;
  }

  std::unique_ptr<BucketStorage::BucketStorageId[]>& getBlocks(){
    return blocks_;
  }

  static pthread_rwlock_t bucketsRwlock_;

 private:
  // Open the next bucket for writes.
  void open(uint32_t next, BucketStorage* storage, uint32_t timeSeriesId);

  uint8_t queriedBucketsAgo_;

  mutable folly::RWSpinLock lock_;
  // mutable folly::MicroSpinLock lock_;

  // Number of points in the active bucket (stream_).
  uint16_t count_;

  // Currently active bucket.
  uint32_t current_;

  // Blocks of metadata for previous data.
  std::unique_ptr<BucketStorage::BucketStorageId[]> blocks_;

  // Current stream of data.
  TimeSeriesStream stream_;

  // This map is used for processing updated data point.
  // The key is timestamp, and the value is the extention and value of the data point.
  std::map<int64_t, folly::fbstring> updatedDataMap_;

  // The read and write lock for updatedDataMap_.
  mutable folly::RWSpinLock updatedDataMapLock_;

  mutable std::vector<folly::MicroSpinLock> updateDataPointLocks_;
};

}
} // facebook::gorilla

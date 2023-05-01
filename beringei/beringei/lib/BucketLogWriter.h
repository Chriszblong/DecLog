/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <mutex>
#include <memory>
#include <thread>
#include <unordered_map>
#include <condition_variable>

// #include "Global.h"
#include "DataLog.h"
#include "FileUtils.h"
#include "BitUtil.h"

#include <folly/MPMCQueue.h>
#include <folly/SmallLocks.h>

namespace facebook {
namespace gorilla {

// extern std::mutex g_mutex;
// extern uint32_t gLSN;
// extern snapshot snapshot_;

class BucketLogWriter {
 public:
  static const std::string kLogFilePrefix;
  static const std::string kCheckpointPrefix;

  BucketLogWriter(
      int windowSize,
      const std::string& dataDirectory,
      size_t queueSize,
      uint32_t allowedTimestampBehind);

  BucketLogWriter(
      int windowSize,
      const std::string& dataDirectory,
      size_t queueSize,
      uint32_t allowedTimestampBehind,
      uint32_t logId);

  ~BucketLogWriter();

  // This will push the given data entry to a queue for logging.
  void logData(int64_t shardId, int32_t index, int64_t unixTime, double value);

  // This will log checkpoint to the persist storage directly.
  void logCheckpoint(int64_t shardId, int32_t index, int64_t unixTime, double value, int32_t flag);

  // This is the NVM version of logData().
  // This will push the given data entry to a queue for logging.
  // The flag indicates whether the data entry is an insertion(flag == 1),
  // an update(flag == 2), or a deletion(flag == 3).
  // After the log entry is inserted into the MPMCQueue, return the LSN. 
  int32_t logData(int64_t shardId, int32_t index, int64_t unixTime, double value, int32_t flag, int64_t oriTime = 0);

  // Writes a single entry from the queue. Does not need to be called
  // if `writerThread` was defined in the constructor.
  bool writeOneLogEntry(bool blockingRead);

  // This function is a NVM version of writeOneLogEntry().
  // Writes a single entry from the queue. Does not need to be called
  // if `writerThread` was defined in the constructor.
  bool writeOneLogEntry_NVM(bool blockingRead, const int threadNum);

  // Starts writing points for this shard.
  void startShard(int64_t shardId);

  // Stops writing points for this shard and closes all the open
  // files.
  void stopShard(int64_t shardId);

  void flushQueue();

  static void startMonitoring();

  static void setNumShards(uint32_t numShards) {
    numShards_ = numShards;
  }

  const std::string* getDataDirectory(){
    return &dataDirectory_;
  }

 private:
  void startWriterThread();
  void stopWriterThread();

  // Calculate LSN by data-driven LSN, used for DNLog.
  uint32_t calculateLSN(int32_t flag, int64_t oriTime);

  // Calculate LSN by global LSN, used for Beringei.
  // However, It maybe unnecessary to calculate LSN explicitly,
  // The codes is just used for test.
  uint32_t calculateLSN(int32_t flag);


  uint32_t bucket(uint64_t unixTime, int shardId) const;
  uint64_t timestamp(uint32_t bucket, int shardId) const;
  uint64_t duration(uint32_t buckets) const;

  // TODO(jdshen): these are temporary, delete when doing bucket staggering.
  uint64_t getRandomNextClearDuration() const;
  uint64_t getRandomOpenNextDuration(int shardId) const;

  // Update the curQueue_.
  void nextQueue();

  struct LogDataInfo {
    int32_t index;

    // Cheating and using only 16-bits for the shard id to save some
    // memory, because currently we are using only 6000 shards.
    int32_t shardId;
    int32_t flag;
    uint32_t LSN;
    int64_t unixTime;
    double value;
  };

  // Convert a log entry to bits without compaction.
  void addLogToBits(const LogDataInfo& info, folly::fbstring&);

  
  uint32_t logId_;

  uint32_t curQueue_;

  int windowSize_;
  folly::MPMCQueue<LogDataInfo> logDataQueue_;
  std::vector<std::unique_ptr<folly::MPMCQueue<LogDataInfo>>> logDataQueues_;
  // folly::MPMCQueue<bool> groupCommitQueue_;
  // std::unique_ptr<std::thread> writerThread_;
  std::vector<std::unique_ptr<std::thread>> writerThreads_;
  std::atomic<bool> stopThread_;
  const std::string dataDirectory_;
  const uint32_t waitTimeBeforeClosing_;
  const uint32_t keepLogFilesAroundTime_;

  struct ShardWriter {
    std::unordered_map<int, std::unique_ptr<DataLogWriter>> logWriters;
    std::shared_ptr<FileUtils> fileUtils;
    uint32_t nextClearTimeSecs;
  };

  // Each shardWriters_ for each log queue to avoid lock contention
  std::vector<std::unordered_map<int64_t, ShardWriter>> shardWriters_;

  static uint32_t numShards_;

  // The mutex used to coordinate active MPMCQueue.
  mutable std::mutex queue_mutex_;

  std::condition_variable queueCon_;

  mutable folly::MicroSpinLock queue_lock_;

  int queueFlag_;

  mutable int lastWriteBuckets_;


  // std::mutex commitMutex_;
  // std::condition_variable commitCon_;
  // std::atomic<int> numOfFlushedBuffers_;
  // // std::atomic<int> numOfInsertedLogs_;
  // uint32_t numOfInsertedLogs_;
};
}
} // facebook:gorilla

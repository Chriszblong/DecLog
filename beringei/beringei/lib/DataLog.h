/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <thread>
#include <stdint.h>
#include <cstdio>
#include <functional>
#include <memory>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <folly/RWSpinLock.h>

#include "FileUtils.h"

namespace facebook {
namespace gorilla {

// The following variants are used as snapshot for parallel logging
struct snapshot
{
  snapshot(int32_t numOfThreads) {
    this->total.assign(numOfThreads, 0);
    this->numbers.assign(numOfThreads, 0);
    this->maxLSNs.assign(numOfThreads, 0);
  };
  // snapshot operator =(snapshot curSnapshot){
  //   this->total = curSnapshot.total;
  //   this->numbers = curSnapshot.numbers;
  //   this->maxLSNs = curSnapshot.maxLSNs;
  // };
  std::vector<uint32_t> total;
  std::vector<uint16_t> numbers;
  std::vector<uint16_t> maxLSNs;
};

extern snapshot snapshot_;

// class DataLogWriter
//
// This class appends data points to a log file.
class DataLogWriter {
 public:
  // Initialize a DataLogWriter which will append data to the given file.
  DataLogWriter(FileUtils::File&& out, int64_t baseTime);

  // This is the NVM version of constructor by using libpmem.
  // Initialize a DataLogWriter which will append data to the given file.
  DataLogWriter(const char *path, int64_t baseTime, uint32_t logId, const int threadID);

  // This is the NVM version of constructor by using libpmem2.
  // Initialize a DataLogWriter which will append data to the given file.
  DataLogWriter(int& fd, int64_t baseTime);

  ~DataLogWriter();

  // Appends a data point to the internal buffer. This operation is
  // not thread safe. Caller is responsible for locking. Data will be
  // written to disk when buffer is full or `flushBuffer` is called or
  // destructor is called.
  void append(uint32_t id, int64_t unixTime, double value);

  // Appends a data point to the internal buffer. This operation is
  // not thread safe. Caller is responsible for locking. Data will be
  // written to disk when buffer is full or `flushBuffer` is called or
  // destructor is called.
  void append_unCompaction(uint32_t id, int64_t unixTime, double value);

  // This is the NVM version of append().
  // Appends a data point to the internal buffer. This operation is
  // not thread safe. Caller is responsible for locking. Data will be
  // written to disk when buffer is full or `flushBuffer` is called or
  // destructor is called.
  bool append(uint32_t id, 
              int64_t unixTime, 
              double value, 
              int16_t flag, 
              uint32_t LSN, 
              uint32_t& numOfFlushedLogs);


  // Flushes the buffer that has been created with `append` calls to
  // disk. Returns true if writing was successful, false otherwise.
  bool flushBuffer();

  // The NVM version of flushBuffer().
  // Flushes the buffer that has been created with `append` calls to
  // NVM. Returns true if writing was successful, false otherwise.
  bool flushBuffer(const int32_t bufferID, const uint32_t LSN, folly::fbstring &lastEntry);

  // Read states from snapshot and wait for 
  // all the logs in other threads with LSNs smaller
  // than the given LSN have been persisted.
  void threadSync(const uint32_t LSN);

  // Reset the the previous values.
  void resetPreviousValues();

  char* getPMemAddr() {
    return pMemAddr_;
  }

  char* getCurPMemAddr() {
    return curPMemAddr_;
  }

  int64_t getFileNameTimestamp(){
    return fileNameTimestamp_;
  }

  int64_t getLastTimestamp() {
    return lastTimestamp_;
  }

  // The condition variable to coordinate logging queues.
  // static std::unique_ptr<std::condition_variable[]> loggingCon_; 

 private:
  // Update the curBuffer_.
  void nextBuffer();

  // It is the NVM file descriptor.
  int fd_;
  // The mapped NVM file address.
  char *pMemAddr_;
  // The current persist address of mapped NVM file.
  char *curPMemAddr_;
  // The length of mapped NVM file.
  size_t mapped_len_;
  // Whether the file is on NVM or not.
  int is_pmem_;


  // The control bit of a log entry aligned by FLAGS_NVM_address_aligned.
  // static char *endOfLogEntry;

  // Get the lock whenever update the pmem address
  static folly::RWSpinLock pmemLock_;

  FileUtils::File out_;
  int64_t lastTimestamp_;
  int64_t fileNameTimestamp_;
  std::unique_ptr<char[]> buffer_;
  size_t bufferSize_;
  std::vector<double> previousValues_;

  // The following variants are used for logging pipeline
  
  std::vector<char*> buffers_;
  // Record the size of each buffer
  std::vector<size_t> bufferSizes_;
  // The current buffer number to record log entry.
  int32_t curBuffer_;
  // Rocord each log entry size in each buffer.
  std::vector<std::vector<size_t>> logEntrySizes_;
  // std::vector<std::unique_ptr<std::thread>> threads;

  folly::fbstring paddingBits_;
  

  // Rocord each log entry bytes for log alignment.
  size_t logEntryBytes_;

  
  // The ID of logging queue for each logging thread.
  int threadID_;

  // static std::vector<folly::RWSpinLock> locks_;
  
  // The ID of logging thread.
  uint32_t logId_;

  // Active logging queue in every logging thread.
  static std::vector<int> flushedFlags_;

  // static std::mutex threadSyncMutex_;

  // int numOfLogs_;
  
};

class DataLogReader {
 public:

  static const int kStartCheckpoint;
  static const int kStopCheckpoint;
  static const int kFinishLSNUpdate;
  static const int kLogFileOffset;
  static const uint32_t kRestartCompaction;

  // Pull all the data points from the file.
  // Returns the number of points read, or -1 if the file could not be read.
  // The callback should return false if reading should be stopped.
  static int readLog(
      const FileUtils::File& file,
      int64_t baseTime,
      std::function<bool(uint32_t, int64_t, double, int16_t, uint32_t)>);

  // This is the NVM version of readLog.
  // Pull all the data points from NVM.
  // Returns the number of points read, or -1 if the file could not be read.
  // The callback should return false if reading should be stopped.
  static int readLog(
      std::vector<boost::filesystem::path>& paths,
      const std::vector<int64_t>& baseTimes,
      const std::vector<int64_t>& offsets,
      const std::vector<int64_t>& preTimestamps,
      uint32_t& curFile,
      std::function<bool(uint32_t, int64_t, double, int16_t, uint32_t)>);

  // Read checkpoints from paths.
  // Returns the timestamp of the last finished checkpoint.
  static int64_t readCheckpoint(
      const std::vector<boost::filesystem::path>& paths,
      std::vector<int64_t>& offsets,
      std::vector<int64_t>& preTimestamps);
};
}
} // facebook:gorilla

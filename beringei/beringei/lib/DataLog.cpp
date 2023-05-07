/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

extern "C"{
#include "libpmem.h"
#include "libpmem2.h"
}
#include <fcntl.h>
#include <thread>
#include <future>
#include <map>
#include "DataLog.h"

#include "FileUtils.h"

#include <folly/GroupVarint.h>

#include "BitUtil.h"
#include "GorillaStatsManager.h"

// DECLARE_int32(num_thrift_pool_threads);


DEFINE_int32(
    bucket_size,
    2 * 3600,
    "Size of each bucket in seconds");

DEFINE_int32(
    log_writer_threads,
    8,
    "The number of log writer threads,"
    "should be the same with that in BeringeiServiceHandler.cpp"
    );

DEFINE_int32(
    max_allowed_LSN,
    // (int32_t)(1<<10 - 1 - FLAGS_num_thrift_pool_threads),
    (int32_t)((1<<10) - 1),
    "Whenever the LSN have reached the maximum number(1023)," 
    "the checkpoint thread will be called.");

DEFINE_int32(
    log_flag_checkpoint,
    4,
    "Indicate the log entry is a checkpoint.");
    
namespace facebook {
namespace gorilla {

// Initialize snapshot for parallel logging.
snapshot snapshot_ = snapshot(FLAGS_log_writer_threads);
// std::vector<folly::RWSpinLock> DataLogWriter::locks_(FLAGS_log_writer_threads);
folly::RWSpinLock DataLogWriter::pmemLock_;

std::vector<int> DataLogWriter::flushedFlags_(FLAGS_log_writer_threads, -1);


// Used for condition variables of parallel logging.
// The size of # smaller than the pramametre FLAGS_log_writer_threads may result in deadlock
// without exception thrown out.
static std::vector<std::mutex> loggingConMutex_(FLAGS_log_writer_threads);

// Initialize condition variables for parallel logging.
// Used for the condition variable.
static std::vector<std::condition_variable> loggingCon_(FLAGS_log_writer_threads);
// std::shared_ptr<std::mutex[]> DataLogWriter::loggingConMutex_( new std::mutex[FLAGS_log_writer_threads] );
// std::unique_ptr<std::condition_variable[]> DataLogWriter::loggingCon_(new std::condition_variable[FLAGS_log_writer_threads]);
// std::vector<std::mutex> DataLogWriter::loggingConMutex_(FLAGS_log_writer_threads);

// std::vector<std::condition_variable> DataLogWriter::loggingCon_(FLAGS_log_writer_threads);
// std::mutex DataLogWriter::threadSyncMutex_;



/* the mapped NVM/DISK log file size -- 1 GB */
// #define NVM_LOG_FILE_SIZE ((size_t)(1 << 30))

/* the mapped NVM/DISK log file size -- 32MB */
#define NVM_LOG_FILE_SIZE ((size_t)(1 << 25))

/* the mapped NVM/DISK checkpoint file size -- 32KB */
#define NVM_CHECKPOINT_FILE_SIZE ((size_t)(1 << 15))

DEFINE_int32(
    number_of_logging_queues,
    2,
    "The number of the buffers for logging pipeline.");

DEFINE_int32(
    NVM_address_aligned,
    1,
    "Whether align log entry by 256 bytes.");

DEFINE_int32(
    log_flag_log_file_offset,
    5,
    "Indicate the log entry is a record of log file offset.");

DEFINE_int32(
    NVM_aligned_bytes_max,
    256,
    "Persist log entry if the remainder is larger than #."
    "The last byte is used for control bits.");

// DEFINE_int32(
//     NVM_aligned_bytes_min,
//     245,
//     "Persist log entry if the current size is larger than #.");

DEFINE_int32(
    NVM_aligned_bits,
    256 * 8,
    "Persist log entry if the remainder is larger than NVM_aligned_bits."
    "The last byte is used for control bits.");

DEFINE_int32(
    Beringei_NVM,
    0,
    "Determine the strategy of sfence(). Beringei_NVM = 1 means using sfence() for each log");

DEFINE_int32(
    data_log_buffer_size,
    256,
    "The size of the internal buffer when logging data. Buffer size of 64K "
    "equals roughly to 3 seconds of data before it's written to disk");
DEFINE_int32(
    max_allowed_timeseries_id,
    10000000,
    "This is the maximum allowed id for a time series in a shard. This is used "
    "for sanity checking that the file isn't corrupt and to avoid allocating "
    "too much memory");

// The algorithm for encoding data tries to take a full use of
// bytes. In the optimal case everything will fit 3 bytes. This is
// possible when the value doesn't change and the timestamp is the
// same as before. If timestamp is different, but value doesn't
// change, it's possible to use 4 bytes. If the value changes, a
// variable number of bytes will be used.

// 2 bits for log flag.
const static int kFlagBits = 2;

// 11 bits with with an unused bits for LSN. One of the bits is used for the control bit.
const static int kLongLSNBits = 10;
const static int kLongLSNBitsForBeringei = 32;
const static int kShortLSNControlBit = 1;
const static int kLongLSNControlBit = 0;

// 3 bytes with with three unused bits. One of the is used for the control bit.
const static int kShortIdBits = 21;

// 4 bytes with with three unused bits. One of the is used for the control bit.
const static int kLongIdBits = 29;
const static int kShortIdControlBit = 0;
const static int kLongIdControlBit = 1;

// 7 + 2 control bits -> 7 bits left in the byte.
const static int kShortDeltaBits = 7;
const static int kShortDeltaMin = -(1 << (kShortDeltaBits - 1)) + 1;
const static int kShortDeltaMax = (1 << (kShortDeltaBits - 1));

// 14 + 3 control bits -> 7 bits left in the byte.
const static int kMediumDeltaBits = 14;
const static int kMediumDeltaMin = -(1 << (kMediumDeltaBits - 1)) + 1;
const static int kMediumDeltaMax = (1 << (kMediumDeltaBits - 1));

const static int kLargeDeltaBits = 32;
const static int32_t kLargeDeltaMin = std::numeric_limits<int32_t>::min();

// Control bits for the timestamp type
const static int kZeroDeltaControlValue = 0; // 0
const static int kShortDeltaControlValue = 2; // 10
const static int kMediumDeltaControlValue = 6; // 110
const static int kLargeDeltaControlValue = 7; // 111

// Control bits for the log entries aligned by NVM_aligned_bytes.
const static uint32_t kEndOfLogEntry = 14; // 1110

// Control bits for the log entries aligned by NVM_aligned_bytes.
const uint32_t DataLogReader::kRestartCompaction = 15; // 1111

const static int kPreviousValuesVectorSizeIncrement = 1000;

const static int kBlockSizeBits = 6;
const static int kLeadingZerosBits = 5;
const static int kMinBytesNeeded = 3;

const static int kSameValueControlBit = 0;
const static int kDifferentValueControlBit = 1;

const static std::string kFailedCounter = ".failed_writes.log";

// Record the offsets of log files after recovery.
static std::map<int64_t, size_t> logFileOffsets;


const int DataLogReader::kStartCheckpoint = -4;
const int DataLogReader::kStopCheckpoint = -5;
const int DataLogReader::kFinishLSNUpdate = -6;
const int DataLogReader::kLogFileOffset = -7;

DataLogWriter::DataLogWriter(FileUtils::File&& out, int64_t baseTime)
    : out_(out),
      lastTimestamp_(baseTime),
      buffer_(new char[FLAGS_data_log_buffer_size]),
      bufferSize_(0),
      logEntryBytes_(0) {
  GorillaStatsManager::addStatExportType(kFailedCounter, SUM);
}

DataLogWriter::DataLogWriter(const char *path, int64_t baseTime, uint32_t logId, const int threadID)
    : lastTimestamp_(baseTime),
      fileNameTimestamp_(baseTime),
      logId_(logId),
      threadID_(threadID),
      logEntryBytes_(0) {
  out_.file = NULL;
  /* Create a pmem file and memory map it */
  // The checkpoint file.
  if (threadID == FLAGS_number_of_logging_queues) {
    if ((pMemAddr_ = (char*) pmem_map_file(path, NVM_CHECKPOINT_FILE_SIZE,
          PMEM_FILE_CREATE|PMEM_FILE_EXCL,
          0666, &mapped_len_, &is_pmem_)) == NULL) {
      perror("can't map pmem_map_file path: ");
      perror(path);
      exit(1);
    }
  }
  // The Log file.
  else {
    std::string curPath(path);
    int64_t newBaseTime = baseTime; 

    // Update baseTime as what recorded in the existing log file.
    if (baseTime % 10) {
      auto itLower = logFileOffsets.lower_bound(baseTime/10);
      auto itUpper = logFileOffsets.upper_bound(baseTime/10);
      if (itLower != logFileOffsets.end() && baseTime - itLower->first < FLAGS_bucket_size && itLower->first % FLAGS_bucket_size != 0) {
        newBaseTime = itLower->first % 10 ? itLower->first : itUpper->first;
        curPath.replace(curPath.rfind(std::to_string(baseTime)), 11, std::to_string(newBaseTime) + std::to_string(threadID));
      }
    }
    // Create a new log file.
    if (logFileOffsets.find(newBaseTime) == logFileOffsets.end()) {
      if ((pMemAddr_ = (char*) pmem_map_file(path, NVM_LOG_FILE_SIZE,
            PMEM_FILE_CREATE|PMEM_FILE_EXCL,
            0666, &mapped_len_, &is_pmem_)) == NULL) {
        perror("can't map pmem_map_file path: ");
        perror(path);
        exit(1);
      }
      curPMemAddr_ = pMemAddr_;
    }
    // The database has recovered from logs and the path exists.
    else {
      if ((pMemAddr_ = (char*) pmem_map_file(curPath.c_str(), NVM_LOG_FILE_SIZE,
            PMEM_FILE_CREATE, 0666, &mapped_len_, &is_pmem_)) == NULL) {
        perror("can't map pmem_map_file path: ");
        perror(path);
        exit(1);
      }
      curPMemAddr_ = pMemAddr_ + logFileOffsets[baseTime];
    }
  }
	
  LOG(INFO) << "is pmem?????" << is_pmem_;

  buffers_.resize(FLAGS_number_of_logging_queues + 1);
  logEntrySizes_.resize(FLAGS_number_of_logging_queues + 1);

  // One more bufferSize is used for checkpoint.
  bufferSizes_.assign(FLAGS_number_of_logging_queues + 1, 0);
  // threads.resize(FLAGS_number_of_logging_queues);

  for (int i =0; i < FLAGS_number_of_logging_queues + 1; ++i){
    buffers_[i] = new char[FLAGS_data_log_buffer_size];
    // bufferSizes_[i] = 0;
    logEntrySizes_[i].resize(0);
    logEntrySizes_[i].reserve(FLAGS_data_log_buffer_size);
  }
  curBuffer_ = 0;

  // Initialize the log entry aligned control bit.
  uint32_t temp = 0;
  BitUtil::addValueToBitString(kEndOfLogEntry, 4, paddingBits_, temp);
  BitUtil::addValueToBitString(0, FLAGS_NVM_aligned_bits, paddingBits_, temp);

  // LOG(INFO) << "Finish dataLog constructor " << lastTimestamp_ << std::endl;
  GorillaStatsManager::addStatExportType(kFailedCounter, SUM);
}

DataLogWriter::DataLogWriter(int& fd, int64_t baseTime)
    : fd_(fd),
      lastTimestamp_(baseTime),
      buffer_(new char[FLAGS_data_log_buffer_size]),
      bufferSize_(0),
      logEntryBytes_(0) {
  GorillaStatsManager::addStatExportType(kFailedCounter, SUM);
}

DataLogWriter::~DataLogWriter() {
  if (out_.file) {
    flushBuffer();
    FileUtils::closeFile(out_.file);
  }
  if (pMemAddr_) {
    folly::fbstring tempBits;
    flushBuffer(curBuffer_, 0, tempBits);
    pmem_unmap(pMemAddr_, mapped_len_);
  }
}

void DataLogWriter::append(uint32_t id, int64_t unixTime, double value) {
  folly::fbstring bits;
  uint32_t numBits = 0;

  if (id > FLAGS_max_allowed_timeseries_id) {
    LOG(ERROR) << "ID too large. Increase max_allowed_timeseries_id?";
    return;
  }

  // Leave two bits unused in the current byte after adding the id to
  // allow the best case scenario of time delta = 0 and value/xor = 0.
  if (id >= (1 << kShortIdBits)) {
    BitUtil::addValueToBitString(kLongIdControlBit, 1, bits, numBits);
    BitUtil::addValueToBitString(id, kLongIdBits, bits, numBits);
  } else {
    BitUtil::addValueToBitString(kShortIdControlBit, 1, bits, numBits);
    BitUtil::addValueToBitString(id, kShortIdBits, bits, numBits);
  }

  // Optimize for zero delta case and increase used bits 8 at a time
  // to fill bytes.
  int64_t delta = unixTime - lastTimestamp_;
  if (delta == 0) {
    BitUtil::addValueToBitString(kZeroDeltaControlValue, 1, bits, numBits);
  } else if (delta >= kShortDeltaMin && delta <= kShortDeltaMax) {
    delta -= kShortDeltaMin;
    CHECK_LT(delta, 1 << kShortDeltaBits);

    BitUtil::addValueToBitString(kShortDeltaControlValue, 2, bits, numBits);
    BitUtil::addValueToBitString(delta, kShortDeltaBits, bits, numBits);
  } else if (delta >= kMediumDeltaMin && delta <= kMediumDeltaMax) {
    delta -= kMediumDeltaMin;
    CHECK_LT(delta, 1 << kMediumDeltaBits);

    BitUtil::addValueToBitString(kMediumDeltaControlValue, 3, bits, numBits);
    BitUtil::addValueToBitString(delta, kMediumDeltaBits, bits, numBits);
  } else {
    delta -= kLargeDeltaMin;
    BitUtil::addValueToBitString(kLargeDeltaControlValue, 3, bits, numBits);
    BitUtil::addValueToBitString(delta, kLargeDeltaBits, bits, numBits);
  }

  if (id >= previousValues_.size()) {
    // If the value hasn't been seen before, assume that the previous
    // value is zero.
    previousValues_.resize(id + kPreviousValuesVectorSizeIncrement, 0);
  }

  uint64_t* v = (uint64_t*)&value;
  uint64_t* previousValue = (uint64_t*)&previousValues_[id];
  uint64_t xorWithPrevious = *v ^ *previousValue;
  if (xorWithPrevious == 0) {
    // Same as previous value, just store a single bit.
    BitUtil::addValueToBitString(kSameValueControlBit, 1, bits, numBits);
  } else {
    BitUtil::addValueToBitString(kDifferentValueControlBit, 1, bits, numBits);

    // Check TimeSeriesStream.cpp for more information about this
    // algorithm.
    int leadingZeros = __builtin_clzll(xorWithPrevious);
    int trailingZeros = __builtin_ctzll(xorWithPrevious);
    if (leadingZeros > 31) {
      leadingZeros = 31;
    }
    int blockSize = 64 - leadingZeros - trailingZeros;
    uint64_t blockValue = xorWithPrevious >> trailingZeros;

    BitUtil::addValueToBitString(
        leadingZeros, kLeadingZerosBits, bits, numBits);
    BitUtil::addValueToBitString(blockSize - 1, kBlockSizeBits, bits, numBits);
    BitUtil::addValueToBitString(blockValue, blockSize, bits, numBits);
  }

  previousValues_[id] = value;
  lastTimestamp_ = unixTime;

  if (bits.length() + bufferSize_ > FLAGS_data_log_buffer_size) {
    flushBuffer();
  }

  memcpy(buffer_.get() + bufferSize_, bits.data(), bits.length());
  bufferSize_ += bits.length();
}

void DataLogWriter::append_unCompaction(uint32_t id, int64_t unixTime, double value) {
  folly::fbstring bits;
  uint32_t numBits = 0;

  if (id > FLAGS_max_allowed_timeseries_id) {
    LOG(ERROR) << "ID too large. Increase max_allowed_timeseries_id?";
    return;
  }

  // Leave two bits unused in the current byte after adding the id to
  // allow the best case scenario of time delta = 0 and value/xor = 0.
  
  BitUtil::addValueToBitString(kLongIdControlBit, 1, bits, numBits);
  BitUtil::addValueToBitString(id, kLongIdBits, bits, numBits);
  

  // Optimize for zero delta case and increase used bits 8 at a time
  // to fill bytes.
  BitUtil::addValueToBitString(kLargeDeltaControlValue, 3, bits, numBits);
  BitUtil::addValueToBitString(unixTime, 64, bits, numBits);

  if (id >= previousValues_.size()) {
    // If the value hasn't been seen before, assume that the previous
    // value is zero.
    previousValues_.resize(id + kPreviousValuesVectorSizeIncrement, 0);
  }

  BitUtil::addValueToBitString(value, 64, bits, numBits);

  previousValues_[id] = value;
  lastTimestamp_ = unixTime;

  if (bits.length() + bufferSize_ > FLAGS_data_log_buffer_size) {
    flushBuffer();
  }

  memcpy(buffer_.get() + bufferSize_, bits.data(), bits.length());
  bufferSize_ += bits.length();
}

void DataLogWriter::resetPreviousValues() {
  previousValues_.assign(previousValues_.size(), 0);
  logEntryBytes_ = 0;
}

bool DataLogWriter::append(
        uint32_t id, 
        int64_t unixTime, 
        double value, 
        int16_t flag, 
        uint32_t LSN, 
        uint32_t& numOfFlushedLogs) {
  // ++numOfLogs_;
  bool flushFlag = false;
  folly::fbstring bits;
  bits.reserve(30);
  uint32_t numBits = 0;

  // LOG(INFO) << "Reserve!!!!!!!!!!!" << std::endl;

  // BitUtil::addValueToBitString(unixTime, 64, bits, numBits);

  // LOG(INFO) << "Append a log entry of shard-" << id << ", flag: " << flag; 

  if (id > FLAGS_max_allowed_timeseries_id) {
    LOG(ERROR) << "ID too large. Increase max_allowed_timeseries_id?";
    return false;
  }

  // Optimize for zero delta case and increase used bits 8 at a time
  // to fill bytes.
  int64_t delta = unixTime - lastTimestamp_;
  if (delta == 0) {
    BitUtil::addValueToBitString(kZeroDeltaControlValue, 1, bits, numBits);
  } else if (delta >= kShortDeltaMin && delta <= kShortDeltaMax) {
    delta -= kShortDeltaMin;
    CHECK_LT(delta, 1 << kShortDeltaBits);

    BitUtil::addValueToBitString(kShortDeltaControlValue, 2, bits, numBits);
    BitUtil::addValueToBitString(delta, kShortDeltaBits, bits, numBits);
  } else if (delta >= kMediumDeltaMin && delta <= kMediumDeltaMax) {
    delta -= kMediumDeltaMin;
    CHECK_LT(delta, 1 << kMediumDeltaBits);

    BitUtil::addValueToBitString(kMediumDeltaControlValue, 3, bits, numBits);
    BitUtil::addValueToBitString(delta, kMediumDeltaBits, bits, numBits);
  } else {
    delta -= kLargeDeltaMin;
    BitUtil::addValueToBitString(kLargeDeltaControlValue, 3, bits, numBits);
    BitUtil::addValueToBitString(delta, kLargeDeltaBits, bits, numBits);
  }

  // Leave two bits unused in the current byte after adding the id to
  // allow the best case scenario of time delta = 0 and value/xor = 0.
  if (id >= (1 << kShortIdBits)) {
    BitUtil::addValueToBitString(kLongIdControlBit, 1, bits, numBits);
    BitUtil::addValueToBitString(id, kLongIdBits, bits, numBits);
  } else {
    BitUtil::addValueToBitString(kShortIdControlBit, 1, bits, numBits);
    BitUtil::addValueToBitString(id, kShortIdBits, bits, numBits);
  }

  if (id >= previousValues_.size()) {
    // If the value hasn't been seen before, assume that the previous
    // value is zero.
    previousValues_.resize(id + kPreviousValuesVectorSizeIncrement, 0);
  }

  uint64_t* v = (uint64_t*)&value;
  uint64_t* previousValue = (uint64_t*)&previousValues_[id];
  uint64_t xorWithPrevious = *v ^ *previousValue;
  if (xorWithPrevious == 0) {
    // Same as previous value, just store a single bit.
    BitUtil::addValueToBitString(kSameValueControlBit, 1, bits, numBits);
  } else {
    BitUtil::addValueToBitString(kDifferentValueControlBit, 1, bits, numBits);

    // Check TimeSeriesStream.cpp for more information about this
    // algorithm.
    int leadingZeros = __builtin_clzll(xorWithPrevious);
    int trailingZeros = __builtin_ctzll(xorWithPrevious);
    if (leadingZeros > 31) {
      leadingZeros = 31;
    }
    int blockSize = 64 - leadingZeros - trailingZeros;
    uint64_t blockValue = xorWithPrevious >> trailingZeros;

    BitUtil::addValueToBitString(
        leadingZeros, kLeadingZerosBits, bits, numBits);
    BitUtil::addValueToBitString(blockSize - 1, kBlockSizeBits, bits, numBits);
    BitUtil::addValueToBitString(blockValue, blockSize, bits, numBits);
  }

  previousValues_[id] = value;
  lastTimestamp_ = unixTime;

  // Convert flag into fbstring.
  BitUtil::addValueToBitString(flag, kFlagBits, bits, numBits);

  // Convert LSN into fbstring.
  if (LSN == 0){
    BitUtil::addValueToBitString(kShortLSNControlBit, 1, bits, numBits);
  }
  else {
    BitUtil::addValueToBitString(kLongLSNControlBit, 1, bits, numBits);
    if (FLAGS_Beringei_NVM) {
      BitUtil::addValueToBitString(LSN, kLongLSNBitsForBeringei, bits, numBits);
    }
    else {
      BitUtil::addValueToBitString(LSN, kLongLSNBits, bits, numBits);
    }
  }

  // LOG(INFO) << "The LSN is: " << LSN << std::endl;
  // LOG(INFO) << "The aligned size is: " << logEntryBytes_ << std::endl;

  // Add the control bit whenever the length of logs is outflow.
  if (FLAGS_NVM_address_aligned){
    if ((logEntryBytes_ + bits.length() > FLAGS_NVM_aligned_bytes_max - 1)) {
      // Record the control bit.
      memcpy(buffers_[curBuffer_] + bufferSizes_[curBuffer_], paddingBits_.data(), FLAGS_NVM_aligned_bytes_max - logEntryBytes_);
      bufferSizes_[curBuffer_] += FLAGS_NVM_aligned_bytes_max - logEntryBytes_;
      // Update the size of the last log entry.
      uint32_t lastLogEntry = logEntrySizes_[curBuffer_].size() - 1;
      logEntrySizes_[curBuffer_][lastLogEntry] += FLAGS_NVM_aligned_bytes_max - logEntryBytes_;
      // Initialize the log entry bytes. 
      logEntryBytes_ = 0;
    }
  }

  // LOG(INFO) << "current log LSN is " << LSN << std::endl;

  // The logging pipeline flushes buffer asynchronously.
  if (bits.length() + bufferSizes_[curBuffer_] > FLAGS_data_log_buffer_size || LSN != 0) {
    
    // bufferSizes_[curBuffer_] = 0;
    // folly::fbstring tempBits("NULL");
    // flushBuffer(curBuffer_, LSN, tempBits);
    if (LSN != 0) {
      // Flush buffer to NVM.
      flushBuffer(curBuffer_, LSN, bits);
    }
    else {
      folly::fbstring tempBits;
      flushBuffer(curBuffer_, LSN, tempBits);
    }
    flushFlag = true;
    // LOG(INFO) << numOfLogs_ <<" logs have been flushed!!!!!! " << std::endl;
    // numOfFlushedLogs = numOfLogs_;
    // numOfLogs_ = 0;
    // LOG(INFO) << "Have flushed!!!!!! " << std::endl;
    logEntryBytes_ = 0;
  }

  // The remaining log entry is processed in the next buffer.
  if (LSN == 0 ){
    // LOG(INFO) << "Memcpy for LSN 0, bits length: " << bits.length(); 
    memcpy(buffers_[curBuffer_] + bufferSizes_[curBuffer_], bits.data(), bits.length());
    bufferSizes_[curBuffer_] += bits.length();

    // Record length for each log entry.
    logEntrySizes_[curBuffer_].emplace_back(bits.length());
    logEntryBytes_ += bits.length() % FLAGS_NVM_aligned_bytes_max;
  }
  return flushFlag;
}

void DataLogWriter::threadSync(const uint32_t LSN){
  // Get the current snapshot of logging threads
  snapshot curSnapshot(snapshot_);

  volatile bool state = false;
  while (!state && FLAGS_log_writer_threads > 1){
    for (int threadID = 0; threadID < FLAGS_log_writer_threads; ++threadID){
      // LOG(INFO) << "Thread sync!!!!! logID: " << logId_ << std::endl;
      if (threadID != logId_) {
        auto threadLSN = snapshot_.maxLSNs[threadID];
        // The maximum LSN of logs in Thread[threadID] is 0.
        // All the types log entries are insertion.
        if (threadLSN == 0){
          state = true;
          continue;
        }
        // The maximum LSN of logs in Thread[threadID] is smaller than LSN.
        // Current logging thread must wait for the logs in Thread[threadID] have been persisted.
        if (threadLSN < LSN ){
          // The logs recorded before threadSync in Thread[threadID] have been persisted.
          if (snapshot_.total[threadID] - curSnapshot.total[threadID] >= curSnapshot.numbers[threadID] ){
            state = true;
          }
          // The logs recorded before threadSync in Thread[threadID] have not been persisted.
          else{
            state = false;
            break;
          }
        }
        // The maximum LSN of logs in Thread[threadID] is larger than LSN.
        else {
          state = true;
        }
      }
    }
  }
}

bool DataLogWriter::flushBuffer(const int32_t bufferID, const uint32_t LSN, folly::fbstring &lastEntry) {
  // LOG(INFO) << "Flushing log buffer!!!!!";
  // LOG(INFO) << "Flushing log buffer!!!!!" << bufferSizes_[bufferID];
  // LOG(INFO) << "Buffer Size: " << bufferSizes_[bufferID] << std::endl;
  if (bufferSizes_[bufferID] > 0) {
    // folly::RWSpinLock::WriteHolder writeGuard(pmemLock_);
    char *bufferAddr = buffers_[bufferID];
    if (is_pmem_) {
      if (FLAGS_NVM_address_aligned == 0){
        // LOG(INFO) << "Flushing log buffer!!!!!" << bufferSizes_[bufferID] << ", current buffer queue is " << bufferID << std::endl;
        for (int i = 0; i < logEntrySizes_[bufferID].size(); ++i) {
          pmem_memcpy_nodrain((void*)curPMemAddr_, bufferAddr, logEntrySizes_[bufferID][i]);
          if (FLAGS_Beringei_NVM == 1) {
            pmem_drain();
          }
          curPMemAddr_ += logEntrySizes_[bufferID][i];
          bufferAddr += logEntrySizes_[bufferID][i];
        }
        // LOG(INFO) << "Finish flushing log buffer!!!!!" << bufferSizes_[bufferID];
      }
      else{
        size_t alignedSize = 0;
        // Align log entries by 256 bytes. 
        for (int i = 0; i < logEntrySizes_[bufferID].size(); ++i) {
          if ((alignedSize + logEntrySizes_[bufferID][i]) == FLAGS_NVM_aligned_bytes_max) {
            pmem_memcpy_nodrain((void*)curPMemAddr_, bufferAddr, FLAGS_NVM_aligned_bytes_max);
            curPMemAddr_ += FLAGS_NVM_aligned_bytes_max;
            bufferAddr += FLAGS_NVM_aligned_bytes_max;
            // if ((alignedSize + logEntrySizes_[bufferID][i]) == FLAGS_NVM_aligned_bytes_max) {
            //   ++i;
            //   alignedSize = 0;
            //   continue;
            // }
            alignedSize = 0;
            continue;
          }
          // else if(i == logEntrySizes_[bufferID].size() - 1) {
          //   alignedSize += logEntrySizes_[bufferID][i];
          //   pmem_memcpy_nodrain((void*)curPMemAddr_, bufferAddr, alignedSize);
          //   curPMemAddr_ += alignedSize;
          //   bufferAddr += alignedSize;
          //   alignedSize = 0;
          // }
          // if (i != logEntrySizes_[bufferID].size() - 1) {
          alignedSize += logEntrySizes_[bufferID][i];
          // }
        }
        // Persist the remaining log entry.
        if (alignedSize != 0){
          if (alignedSize == FLAGS_NVM_aligned_bytes_max || alignedSize + lastEntry.length() > FLAGS_NVM_aligned_bytes_max - 1) {
            pmem_memcpy_nodrain((void*)curPMemAddr_, bufferAddr, FLAGS_NVM_aligned_bytes_max);
            curPMemAddr_ += FLAGS_NVM_aligned_bytes_max;
            bufferAddr += FLAGS_NVM_aligned_bytes_max;
          }
          else {
            pmem_memcpy_nodrain((void*)curPMemAddr_, bufferAddr, alignedSize);
            curPMemAddr_ += alignedSize;
            bufferAddr += alignedSize;
            alignedSize = 0;
          }
        }
      }
      // Wait for the above log entries persist.
      pmem_drain();
    }
    else {
      if (FLAGS_NVM_address_aligned == 0){
        // LOG(INFO) << "Start flush buffer!!!! bits length: " << bufferSizes_[bufferID] << ", buffersize: " << bufferSizes_[bufferID] << std::endl;
        memcpy(curPMemAddr_, buffers_[bufferID], bufferSizes_[bufferID]);
        if (pmem_msync(curPMemAddr_, bufferSizes_[bufferID]) < 0) {
          perror("pmem_msync");
          // exit(1);
        }
        curPMemAddr_ += bufferSizes_[bufferID];
      }
      else {
        size_t alignedSize = 0;
        // Align log entries by 256 bytes. 
        for (int i = 0; i < logEntrySizes_[bufferID].size(); ++i) {
          if ((alignedSize + logEntrySizes_[bufferID][i]) == FLAGS_NVM_aligned_bytes_max) {
            memcpy((void*)curPMemAddr_, bufferAddr, FLAGS_NVM_aligned_bytes_max);
            if (pmem_msync(curPMemAddr_, FLAGS_NVM_aligned_bytes_max) < 0) {
              perror("pmem_msync");
              // exit(1);
            }
            curPMemAddr_ += FLAGS_NVM_aligned_bytes_max;
            bufferAddr += FLAGS_NVM_aligned_bytes_max;
            alignedSize = 0;
            continue;
          }
          // else if(i == logEntrySizes_[bufferID].size() - 1) {
          //   alignedSize += logEntrySizes_[bufferID][i];
          //   pmem_memcpy_nodrain((void*)curPMemAddr_, bufferAddr, alignedSize);
          //   curPMemAddr_ += alignedSize;
          //   bufferAddr += alignedSize;
          //   alignedSize = 0;
          // }
          // if (i != logEntrySizes_[bufferID].size() - 1) {
          alignedSize += logEntrySizes_[bufferID][i];
          // }
        }
        // Persist the remaining log entry.
        if (alignedSize != 0){
          if (alignedSize == FLAGS_NVM_aligned_bytes_max || alignedSize + lastEntry.length() > FLAGS_NVM_aligned_bytes_max - 1) {
            memcpy((void*)curPMemAddr_, bufferAddr, FLAGS_NVM_aligned_bytes_max);
            if (pmem_msync(curPMemAddr_, FLAGS_NVM_aligned_bytes_max) < 0) {
              perror("pmem_msync");
              // exit(1);
            }
            curPMemAddr_ += FLAGS_NVM_aligned_bytes_max;
            bufferAddr += FLAGS_NVM_aligned_bytes_max;
          }
          else {
            memcpy((void*)curPMemAddr_, bufferAddr, alignedSize);
            if (pmem_msync(curPMemAddr_, alignedSize) < 0) {
              perror("pmem_msync");
              exit(1);
            }
            curPMemAddr_ += alignedSize;
            bufferAddr += alignedSize;
            alignedSize = 0;
          }
        }
      }
    }
    bufferSizes_[bufferID] = 0;
    logEntrySizes_[bufferID].clear();
  }
  
  if (lastEntry.size() != 0){
    // Persist checkpoint log.
    if (LSN == FLAGS_max_allowed_LSN + 1) {
      if (is_pmem_)
        pmem_memcpy_persist(curPMemAddr_, lastEntry.data(), lastEntry.length());
      else {
        memcpy(curPMemAddr_, lastEntry.data(), lastEntry.length());
        pmem_msync(curPMemAddr_, lastEntry.length());
      }
      curPMemAddr_ += lastEntry.length();
    }
    // Persist logs by pipeline.
    else {
      std::unique_lock<std::mutex> conGuard(loggingConMutex_[logId_]);
      
      // LOG(INFO) << "thread id: " << std::this_thread::get_id() << std::endl;

      // Wait until the other thread have flushed the log with LSN > 0.
      while (threadID_ - flushedFlags_[logId_] != 1 && flushedFlags_[logId_] - threadID_ != FLAGS_number_of_logging_queues - 1){
        // queueCon_.notify_all();
        loggingCon_[logId_].wait(conGuard);
      }
      // loggingCon_[logId_].wait(conGuard, [&](){return flushedFlags_[logId_];});
      
      flushedFlags_[logId_] = threadID_;

      // LOG(INFO) << threadID_ << std::endl;

      // Synchronize logging threads by snapshot.
      threadSync(LSN);

      // Update the maximum LSN for the current logging thread.
      // snapshot_.maxLSNs[logId_] = LSN;

      // Persist current log with LSN larger than 0.
      if (is_pmem_)
        pmem_memcpy_persist(curPMemAddr_, lastEntry.data(), lastEntry.length());
      else {
        memcpy(curPMemAddr_, lastEntry.data(), lastEntry.length());
        pmem_msync(curPMemAddr_, lastEntry.length());
      }
      curPMemAddr_ += lastEntry.length();
      // Update total number of logs with LSN larger than 0 for the current logging thread.
      snapshot_.total[logId_] += 1;
      // Update the number of logs with LSN larger than 0 for the current logging thread.
      snapshot_.numbers[logId_] -= 1;
      // The log with LSN larger than 0 has been persisted.
      if (snapshot_.numbers[logId_] == 0) {
        snapshot_.maxLSNs[logId_] = 0;
      }

      // Notify the other tread to continue flush 
      loggingCon_[logId_].notify_all();
      // queueCon_.notify_all();
    }
  }
  
  return true;
}

void DataLogWriter::nextBuffer() {
  curBuffer_ = curBuffer_ + 1 == FLAGS_number_of_logging_queues ? 0 : curBuffer_ + 1;
}

bool DataLogWriter::flushBuffer() {
  bool success = true;
  if (bufferSize_ > 0) {
    int written = fwrite(buffer_.get(), sizeof(char), bufferSize_, out_.file);
    fflush(out_.file);
    fsync(out_.file->_fileno);
    if (written != bufferSize_) {
      PLOG(ERROR) << "Flushing buffer failed! Wrote " << written << " of "
                  << bufferSize_ << " to " << out_.name;
      GorillaStatsManager::addStatValue(kFailedCounter, 1);
      success = false;
    }
    bufferSize_ = 0;
  }

  return success;
}

int DataLogReader::readLog(
    const FileUtils::File& file,
    int64_t baseTime,
    std::function<bool(uint32_t, int64_t, double, int16_t, uint32_t)> out) {
  fseek(file.file, 0, SEEK_END);
  size_t len = ftell(file.file);
  if (len == 0) {
    return 0;
  }

  std::unique_ptr<char[]> buffer(new char[len]);
  fseek(file.file, 0, SEEK_SET);
  if (fread(buffer.get(), 1, len, file.file) != len) {
    PLOG(ERROR) << "Failed to read entire file " << file.name;
    return -1;
  }

  // Read out all the available points.
  int points = 0;
  int64_t prevTime = baseTime;
  std::vector<double> previousValues;
  uint64_t bitPos = 0;

  // Need at least three bytes for a complete value.
  while (bitPos <= len * 8 - kMinBytesNeeded * 8) {
    try {
      // Read the id of the time series.
      int idControlBit =
          BitUtil::readValueFromBitString(buffer.get(), len, bitPos, 1);
      uint32_t id;
      if (idControlBit == kShortIdControlBit) {
        id = BitUtil::readValueFromBitString(
            buffer.get(), len, bitPos, kShortIdBits);
      } else {
        id = BitUtil::readValueFromBitString(
            buffer.get(), len, bitPos, kLongIdBits);
      }

      if (id > FLAGS_max_allowed_timeseries_id) {
        LOG(ERROR) << "Corrupt file. ID is too large " << id;
        break;
      }

      // Read the time stamp delta based on the the number of bits in
      // the delta.
      uint32_t timeDeltaControlValue =
          BitUtil::readValueThroughFirstZero(buffer.get(), len, bitPos, 3);
      int64_t timeDelta = 0;
      switch (timeDeltaControlValue) {
        case kZeroDeltaControlValue:
          break;
        case kShortDeltaControlValue:
          timeDelta = BitUtil::readValueFromBitString(
                          buffer.get(), len, bitPos, kShortDeltaBits) +
              kShortDeltaMin;
          break;
        case kMediumDeltaControlValue:
          timeDelta = BitUtil::readValueFromBitString(
                          buffer.get(), len, bitPos, kMediumDeltaBits) +
              kMediumDeltaMin;
          break;
        case kLargeDeltaControlValue:
          timeDelta = BitUtil::readValueFromBitString(
                          buffer.get(), len, bitPos, kLargeDeltaBits) +
              kLargeDeltaMin;
          break;
        default:
          LOG(ERROR) << "Invalid time delta control value "
                     << timeDeltaControlValue;
          return points;
      }

      int64_t unixTime = prevTime + timeDelta;
      prevTime = unixTime;

      if (id >= previousValues.size()) {
        previousValues.resize(id + kPreviousValuesVectorSizeIncrement, 0);
      }

      // Finally read the value.
      double value;
      uint32_t sameValueControlBit =
          BitUtil::readValueFromBitString(buffer.get(), len, bitPos, 1);
      if (sameValueControlBit == kSameValueControlBit) {
        value = previousValues[id];
      } else {
        uint32_t leadingZeros = BitUtil::readValueFromBitString(
            buffer.get(), len, bitPos, kLeadingZerosBits);
        uint32_t blockSize = BitUtil::readValueFromBitString(
                                 buffer.get(), len, bitPos, kBlockSizeBits) +
            1;
        uint64_t blockValue = BitUtil::readValueFromBitString(
            buffer.get(), len, bitPos, blockSize);

        // Shift to left by the number of trailing zeros
        blockValue <<= (64 - blockSize - leadingZeros);

        uint64_t* previousValue = (uint64_t*)&previousValues[id];
        uint64_t xorredValue = blockValue ^ *previousValue;
        double* temp = (double*)&xorredValue;
        value = *temp;
      }

      previousValues[id] = value;

      // Read the flag.
      int16_t flag = BitUtil::readValueFromBitString(
            buffer.get(), len, bitPos, kFlagBits);

      if (flag < 1){
        LOG(ERROR) << "Invalid log flag value "
                     << flag;
          return points;
      }
      
      // Read the LSN.
      uint32_t LSN;
      uint32_t LSNControlBit =
          BitUtil::readValueFromBitString(buffer.get(), len, bitPos, 1);
      if (LSNControlBit == 0){
        LSN = 0;
      }
      else {
        if (FLAGS_Beringei_NVM) {
          LSN = BitUtil::readValueFromBitString(
            buffer.get(), len, bitPos, kLongLSNBitsForBeringei);
        }
        else {
          LSN = BitUtil::readValueFromBitString(
            buffer.get(), len, bitPos, kLongLSNBits);
        }
        if (LSN < 0){
          LOG(ERROR) << "Invalid LSN value "
                      << LSN;
            return points;
        }
      }

      // Each tuple (id, unixTime, value) in the file is byte aligned.
      if (bitPos % 8 != 0) {
        bitPos += 8 - (bitPos % 8);
      }

      if (!out(id, unixTime, value, flag, LSN)) {
        // Callback doesn't accept more points.
        break;
      }
      points++;

    } catch (std::exception& e) {
      // Most likely too many bits were being read.
      LOG(ERROR) << e.what();
      break;
    }
  }

  return points;
}

int DataLogReader::readLog(
    std::vector<boost::filesystem::path>& paths,
    const std::vector<int64_t>& baseTimes,
    const std::vector<int64_t>& offsets,
    const std::vector<int64_t>& preTimestamps,
    uint32_t& curFile,
    std::function<bool(uint32_t, int64_t, double, int16_t, uint32_t)> out) {

  // The mapped address of log files.
  std::vector<char *> pMemAddrs(paths.size());

  // The length of mapped files.
  std::vector<size_t> mapped_lens(paths.size());

  // The maximum length for log analysis.
  std::vector<size_t> maxLens(paths.size());

  // Current postion of bits to read.
  std::vector<uint64_t> bitPoses(paths.size(), 0);

  // The begin bit position of each log entry.
  std::vector<size_t> logEntryBeginPoses(paths.size(), 0);

  // Record the previous timestamp.
  std::vector<int64_t> prevTimes(preTimestamps);

  // Record the previous value for decompression.
  std::vector<std::vector<double>> previousValues(paths.size());

  // Flags to indicates whether the logs have been finished read.
  std::vector<bool> endOfFiles(paths.size(), 0);

  int is_pmem;

  for (int i = 0; i < paths.size(); ++i) {
    /* create a pmem file and memory map it */
    if (((pMemAddrs[i]) = (char*) pmem_map_file(paths[i].c_str(), NVM_LOG_FILE_SIZE,
          PMEM_FILE_CREATE,
          0666, &(mapped_lens[i]), &is_pmem)) == NULL) {
      perror("can't map pmem_map_file path: ");
      perror(paths[i].c_str());
      return -1;
    }

    // Need at least three bytes for a complete value.
    
    // if (FLAGS_NVM_address_aligned){
    //   maxLens[i] = (mapped_lens[i] - 256) * 8;
    // }
    // else{
    maxLens[i] = (mapped_lens[i] - kMinBytesNeeded * 8) * 8;

    // Redo logs from offsets which recorded by the last checkpoint.
    // if (baseTimes[i] < 1438000300) {
    //   bitPoses[i] = 0;
    //   prevTimes[i] = baseTimes[i];
    // }
    // else {
    bitPoses[i] = offsets[i] * 8;
    // }
    // }
  }

  // Read out all the available points.
  int points = 0;

  size_t alignedSize = 0;
  // size_t logEntryBeginPos = 0;
  
  while (bitPoses[curFile] <= maxLens[curFile]) {
    // LOG(INFO) << "bitPos is: " << bitPos << std::endl;
    // if(bitPos == 357626824) {
    //   bitPos += 1;
    //   bitPos -= 1;
    // }
    // if(points == 104) {
    //   points += 1;
    //   points -= 1;
    // }
    try {
      // if (FLAGS_NVM_address_aligned) {
      logEntryBeginPoses[curFile] = bitPoses[curFile];
      // }

      // Read the time stamp delta based on the the number of bits in
      // the delta.
      uint32_t timeDeltaControlValue =
          BitUtil::readValueThroughFirstZero(pMemAddrs[curFile], mapped_lens[curFile], bitPoses[curFile], 4);

      int64_t timeDelta = 0;
      switch (timeDeltaControlValue) {
        case kZeroDeltaControlValue:
          break;
        case kShortDeltaControlValue:
          timeDelta = BitUtil::readValueFromBitString(
                          pMemAddrs[curFile], mapped_lens[curFile], bitPoses[curFile], kShortDeltaBits) +
              kShortDeltaMin;
          break;
        case kMediumDeltaControlValue:
          timeDelta = BitUtil::readValueFromBitString(
                          pMemAddrs[curFile], mapped_lens[curFile], bitPoses[curFile], kMediumDeltaBits) +
              kMediumDeltaMin;
          break;
        case kLargeDeltaControlValue:
          timeDelta = BitUtil::readValueFromBitString(
                          pMemAddrs[curFile], mapped_lens[curFile], bitPoses[curFile], kLargeDeltaBits) +
              kLargeDeltaMin;
          break;

        // End of the aligned log entries, jump to the next aligned bits.
        case kEndOfLogEntry:
          bitPoses[curFile] +=  (bitPoses[curFile] & 0x7 == 0) ? 0 : (8 - (bitPoses[curFile] & 0x7));
          alignedSize += (bitPoses[curFile] - logEntryBeginPoses[curFile]);
          bitPoses[curFile] += (alignedSize == FLAGS_NVM_aligned_bits) ? 0 : (FLAGS_NVM_aligned_bits - alignedSize % (FLAGS_NVM_aligned_bits));
          alignedSize = 0;
          continue;
        
        // A checkpoint has reset the compaction values.
        case kRestartCompaction:
          previousValues[curFile].assign(previousValues[curFile].size(), 0);
          bitPoses[curFile] +=  (bitPoses[curFile] & 0x7 == 0) ? 0 : (8 - (bitPoses[curFile] & 0x7));
          alignedSize = 0;
          continue;

        default:
          LOG(ERROR) << "Invalid time delta control value "
                     << timeDeltaControlValue;
          for (int i = 0; i < logEntryBeginPoses.size(); ++i) {
            logFileOffsets.insert(std::make_pair(baseTimes[i], logEntryBeginPoses[i]));
          }
          return points;
      }

      // The log info must be updated here to avoid errors.
      int64_t unixTime = prevTimes[curFile] + timeDelta;
      prevTimes[curFile] = unixTime;

      // Read the id of the time series.
      int idControlBit =
          BitUtil::readValueFromBitString(pMemAddrs[curFile], mapped_lens[curFile], bitPoses[curFile], 1);
      uint32_t id;
      if (idControlBit == kShortIdControlBit) {
        id = BitUtil::readValueFromBitString(
            pMemAddrs[curFile], mapped_lens[curFile], bitPoses[curFile], kShortIdBits);
      } else {
        id = BitUtil::readValueFromBitString(
            pMemAddrs[curFile], mapped_lens[curFile], bitPoses[curFile], kLongIdBits);
      }

      if (id > FLAGS_max_allowed_timeseries_id) {
        LOG(ERROR) << "Corrupt file. ID is too large " << id;
        LOG(INFO) << "The number of points is "
                  << points << std::endl;
        break;
      }

      if (id >= previousValues[curFile].size()) {
        previousValues[curFile].resize(id + kPreviousValuesVectorSizeIncrement, 0);
      }

      // Read the value.
      double value;
      uint32_t sameValueControlBit =
          BitUtil::readValueFromBitString(pMemAddrs[curFile], mapped_lens[curFile], bitPoses[curFile], 1);
      
      if (sameValueControlBit == kSameValueControlBit) {
        value = previousValues[curFile][id];
      } else {
        uint32_t leadingZeros = BitUtil::readValueFromBitString(
            pMemAddrs[curFile], mapped_lens[curFile], bitPoses[curFile], kLeadingZerosBits);
        uint32_t blockSize = BitUtil::readValueFromBitString(
                                pMemAddrs[curFile], mapped_lens[curFile], bitPoses[curFile], kBlockSizeBits) +
            1;
        uint64_t blockValue = BitUtil::readValueFromBitString(
            pMemAddrs[curFile], mapped_lens[curFile], bitPoses[curFile], blockSize);

        // Shift to left by the number of trailing zeros
        blockValue <<= (64 - blockSize - leadingZeros);

        uint64_t* previousValue = (uint64_t*)&(previousValues[curFile][id]);
        uint64_t xorredValue = blockValue ^ *previousValue;
        double* temp = (double*)&xorredValue;
        value = *temp;
      }

      previousValues[curFile][id] = value;
      
      // Read the flag.
      int16_t flag = BitUtil::readValueFromBitString(
            pMemAddrs[curFile], mapped_lens[curFile], bitPoses[curFile], kFlagBits);

      // The id, timestamp, and value maybe zero at the same time.
      // However, the flag bit can not be 0. So we use it to determine
      // whether this is the last log entry.  
      if (flag == 0) {
        endOfFiles[curFile] = true;
        alignedSize = 0;
        // Check whether all the logs for the current bucket have finished reading.
        bool finishRead = true;
        for (int i = 0; i < paths.size(); ++i) {
          if (!endOfFiles[i]) {
            finishRead = false;
            break;
          }
        }

        // Finished
        if (finishRead) {
          LOG(INFO) << "Finished read log file  "
                  << paths[curFile] << std::endl;
          LOG(INFO) << "The number of points is "
                    << points << std::endl;

          for (int i = 0; i < logEntryBeginPoses.size(); ++i) {
            logFileOffsets.insert(std::make_pair(baseTimes[i], logEntryBeginPoses[i]));
          }
          return points;
        }
      }
      uint32_t LSN = 0;
      if (!endOfFiles[curFile]) {
          if (flag < 1){
          LOG(INFO) << "Invalid log flag value "
                      << flag;
          for (int i = 0; i < logEntryBeginPoses.size(); ++i) {
            logFileOffsets.insert(std::make_pair(baseTimes[i], logEntryBeginPoses[i]));
          }
          return points;
        }

        
        // Read the LSN.
        uint32_t LSNControlBit =
            BitUtil::readValueFromBitString(pMemAddrs[curFile], mapped_lens[curFile], bitPoses[curFile], 1);
        if (LSNControlBit == kShortLSNControlBit){
          LSN = 0;
        }
        else {
          if (FLAGS_Beringei_NVM) {
            LSN = BitUtil::readValueFromBitString(
              pMemAddrs[curFile], mapped_lens[curFile], bitPoses[curFile], kLongLSNBitsForBeringei);
          }
          else {
            LSN = BitUtil::readValueFromBitString(
              pMemAddrs[curFile], mapped_lens[curFile], bitPoses[curFile], kLongLSNBits);
          }
          
          if (LSN < 0){
            LOG(ERROR) << "Invalid LSN value "
                        << LSN;

            for (int i = 0; i < logEntryBeginPoses.size(); ++i) {
              logFileOffsets.insert(std::make_pair(baseTimes[i], logEntryBeginPoses[i]));
            }
            return points;
          }
        }

        if (flag > 1 && (LSN < 1 || (LSN > FLAGS_max_allowed_LSN && !FLAGS_Beringei_NVM))){
          LOG(INFO) << "Invalid log flag value "
                      << flag << " and invalid LSN: " << LSN;
          
          for (int i = 0; i < logEntryBeginPoses.size(); ++i) {
            logFileOffsets.insert(std::make_pair(baseTimes[i], logEntryBeginPoses[i]));
          }
          return points;
        }

        // LOG(INFO) << "The LSN is: " << LSN << std::endl;

        // Each tuple (id, unixTime, value, flag, LSN) in the file is byte aligned.
        if ((bitPoses[curFile] & 0x7) != 0) {
          bitPoses[curFile] += 8 - (bitPoses[curFile] & 0x7);
        }

        if (FLAGS_NVM_address_aligned) {
          
          // If the bit position is larger than FLAGS_NVM_aligned_bits or the flag read is zero,
          // Align log entry with FLAGS_NVM_aligned_bytes bytes.
          // if (alignedSize + (bitPos - logEntryBeginPos) > FLAGS_NVM_aligned_bits || (flag == 0 && !isEndOfLog)) {
          //   if (alignedSize + (bitPos - logEntryBeginPos) > FLAGS_NVM_aligned_bits) {
          //     bitPos -= bitPos % (FLAGS_NVM_aligned_bits);
          //   }
          //   else {
          //     bitPos += FLAGS_NVM_aligned_bits - bitPos % (FLAGS_NVM_aligned_bits);
          //   }
          //   isEndOfLog = true;
          //   alignedSize = 0;
          //   continue;
          // }
          // isEndOfLog = false;
          if (LSN == 0) {
            alignedSize += (bitPoses[curFile] - logEntryBeginPoses[curFile]);
          }
          else {
            alignedSize = 0;
          }
          // LOG(INFO) << "The aligned size is: " << alignedSize/8 << std::endl;
        }

        if (!out(id, unixTime, value, flag, LSN)) {
          // Callback doesn't accept more points.
          break;
        }
        ++points;
      }

      

      // Read logs from the next file.
      if(LSN > 0 || endOfFiles[curFile]){
        do {
          curFile = curFile + 1 == paths.size() ? 0 : curFile + 1;
        }
        while(endOfFiles[curFile]);
      }

    } catch (std::exception& e) {
      // Most likely too many bits were being read.
      LOG(ERROR) << e.what();
      break;
    }
  }
  for(int i = 0; i < paths.size(); ++i) {
    pmem_unmap(pMemAddrs[i], mapped_lens[i]);
  }
  for (int i = 0; i < logEntryBeginPoses.size(); ++i) {
    logFileOffsets.insert(std::make_pair(baseTimes[i], logEntryBeginPoses[i]));
  }
  return points;
}

int64_t DataLogReader::readCheckpoint(
    const std::vector<boost::filesystem::path>& paths, 
    std::vector<int64_t>& offsets,
    std::vector<int64_t>& preTimestamps) {
  // The mapped address of log files.
  char * pMemAddr;

  // The length of mapped files.
  size_t mapped_len;

  // The maximum length for log analysis.
  size_t maxLen;

  // Current postion of bits to read.
  uint64_t bitPos = 0;

  // Record the previous timestamp.
  int64_t lastFinishedCheckpointTime = 0;

  // Record the previous timestamp.
  int64_t curCheckpointTime = -1;

  // Record the offsets for each log file.
  std::vector<int64_t> curOffsets(offsets);

  // Record the previous timestamp for each log file.
  std::vector<int64_t> curPreTimestamps(preTimestamps);

  int is_pmem;

  // Read checkpoint in reversed order.
  for (auto pathIt = paths.rbegin(); pathIt != paths.rend(); ++pathIt) {
    /* create a pmem file and memory map it */
    if (((pMemAddr) = (char*) pmem_map_file(pathIt->string().c_str(), NVM_CHECKPOINT_FILE_SIZE,
          PMEM_FILE_CREATE,
          0666, &(mapped_len), &is_pmem)) == NULL) {
      perror("can't map pmem_map_file path: ");
      perror(pathIt->c_str());
      return -1;
    }

    // Need at least 28 bytes for a complete value.
    maxLen = (mapped_len - 28) * 8;

    int curLogID = 0;

    // Read each checkpoint from the checkpoint log file and return the last finished checkpoint.
    while (bitPos <= maxLen) {
      try {
        // Read the checkpoint flag.
        int32_t checkPointFlag = BitUtil::readValueFromBitString(pMemAddr, mapped_len, bitPos, 32);

        if(checkPointFlag != kStartCheckpoint && checkPointFlag != kFinishLSNUpdate && \
          checkPointFlag != kStopCheckpoint && checkPointFlag != kLogFileOffset && \
          lastFinishedCheckpointTime != 0) {
          break;
        }
        
        // Read the checkpoint timestamp
        int64_t curTimestamp = BitUtil::readValueFromBitString(pMemAddr, mapped_len, bitPos, 64);

        // Read the value.
        uint64_t value = BitUtil::readValueFromBitString(pMemAddr, mapped_len, bitPos, 64);

        // Read the log flag.
        uint32_t logFlag = BitUtil::readValueFromBitString(pMemAddr, mapped_len, bitPos, 32);

        if (logFlag != FLAGS_log_flag_checkpoint && logFlag != FLAGS_log_flag_log_file_offset && \
             lastFinishedCheckpointTime != 0) {
          break;
        }

        //Read the LSN.
        uint32_t LSN = BitUtil::readValueFromBitString(pMemAddr, mapped_len, bitPos, 32);

        if (LSN != FLAGS_max_allowed_LSN + 1 && lastFinishedCheckpointTime != 0) {
          break;
        }

        switch (checkPointFlag) {
          case kLogFileOffset:
            curOffsets[curLogID] = value;
            curPreTimestamps[curLogID] = curTimestamp;
            ++curLogID;
            break;
            
          case kStartCheckpoint:
            curCheckpointTime = curTimestamp;
            break;

          // The checkpoint has finished.
          case kStopCheckpoint:
            lastFinishedCheckpointTime = curCheckpointTime;
            curCheckpointTime = 0;
            curLogID = 0;
            offsets.swap(curOffsets);
            preTimestamps.swap(curPreTimestamps);
            break;

          case kFinishLSNUpdate:
            // The kFinishLSNUpdate is not used for now.
            break;

          default:
            // Unreadable checkpoint.
            break;
        }

        // Each tuple (id, unixTime, value, flag, LSN) in the file is byte aligned.
        if ((bitPos & 0x7) != 0) {
          bitPos += 8 - (bitPos & 0x7);
        }
      } catch (std::exception& e) {
        // Most likely too many bits were being read.
        LOG(ERROR) << e.what();
        break;
      }
    }
    pmem_unmap(pMemAddr, mapped_len);
    if (lastFinishedCheckpointTime != 0) {
      return lastFinishedCheckpointTime;
    }
  }
  return lastFinishedCheckpointTime;
}
}
} // facebook:gorilla



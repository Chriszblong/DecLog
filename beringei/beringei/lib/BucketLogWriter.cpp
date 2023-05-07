/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <fcntl.h>
#include <sys/prctl.h>
#include <map>
#include "BucketLogWriter.h"

#include "BucketUtils.h"
#include "GorillaStatsManager.h"
#include "GorillaTimeConstants.h"

#include "glog/logging.h"

DEFINE_int32(
    using_libpmem_or_libpmem2,
    1,
    "Using libpmem.h(1) or libpmem2.h(2).");

DEFINE_int32(
    no_logging,
    0,
    "The function logdata() return directly.");

DEFINE_int32(
    is_checkpoint,
    0,
    "Whether to activate the checkpoint thread, 1 is active and 0 is inactive");
  
DECLARE_int32(log_writer_threads);

DECLARE_int32(max_allowed_LSN);

DECLARE_int32(log_flag_checkpoint);

namespace facebook {
namespace gorilla {

DECLARE_int32(data_log_buffer_size);

DECLARE_int32(Beringei_NVM);

DECLARE_int32(number_of_logging_queues);


DECLARE_int32(log_flag_log_file_offset);

DEFINE_int32(
    group_commit_queue_size,
    10,
    "Determine that after # buffers persisted, the transactions/operations can be committed"
    "The size must be set compatible with data_log_buffer_size."
    "Otherwise, there maybe a deadlock!!!!");

DEFINE_int32(
    using_libpmem,
    1,
    "Using libpmem.h.");

DEFINE_int32(
    using_libpmem2,
    2,
    "Using libpmem2.h. The codes for libpmem2 have not finished.");

DEFINE_int32(
    log_flag_insertion,
    1,
    "Indicate the log entry is a data insertion.");

DEFINE_int32(
    log_flag_update,
    2,
    "Indicate the log entry is a data update.");

DEFINE_int32(
    log_flag_deletion,
    3,
    "Indicate the log entry is a data deletion.");

DEFINE_int32(
    unix_time_bits,
    52,
    "52 bits are used for unixTime in extension timestamp.");

// 52 bits are used for unixTime in extension timestamp.
// static const int unixTimeBits = 52;

static const int kLogFileBufferSize = FLAGS_data_log_buffer_size;
static const std::string kLogDataFailures = ".log_data_failures";
static const std::string kLogFilesystemFailures =
    ".failed_writes.log_filesystem";

// These are not valid indexes so they can be used to control starting
// and stopping shards.
static const int kStartShardIndex = -1;
static const int kStopShardIndex = -2;
static const int kNoOpIndex = -3;

static const int kMaxActiveBuckets = 2;

static const int kFileOpenRetries = 5;
static const int kSleepUsBetweenFailures = 100 * kGorillaUsecPerMs; // 100 us
static const float kWaiteForLogsPersist = 0.1; // 0.1s
const std::string BucketLogWriter::kLogFilePrefix = "log";
const std::string BucketLogWriter::kCheckpointPrefix = "checkpoint";


static std::mutex g_mutex;
// std::atomic<uint32_t> gLSN(0);
static uint32_t gLSN = 0;

// std::unique_ptr<std::mutex[]> BucketLogWriter::queue_mutexes_(new std::mutex[FLAGS_log_writer_threads]);

uint32_t BucketLogWriter::numShards_ = 1;

BucketLogWriter::BucketLogWriter(
    int windowSize,
    const std::string& dataDirectory,
    size_t queueSize,
    uint32_t allowedTimestampBehind)
    : windowSize_(windowSize),
      logDataQueue_(queueSize),
      // writerThread_(nullptr),
      stopThread_(false),
      dataDirectory_(dataDirectory),
      queueFlag_(-1),

      // One `allowedTimestampBehind` delay to allow the data to come in
      // and one more delay to allow the data to be dequeued and written.
      waitTimeBeforeClosing_(allowedTimestampBehind * 2),
      keepLogFilesAroundTime_(BucketUtils::duration(2, windowSize)) {
  CHECK_GT(windowSize, allowedTimestampBehind)
      << "Window size " << windowSize
      << " must be larger than allowedTimestampBehind "
      << allowedTimestampBehind;
  queue_lock_.init();

  startWriterThread();
}

BucketLogWriter::BucketLogWriter(
    int windowSize,
    const std::string& dataDirectory,
    size_t queueSize,
    uint32_t allowedTimestampBehind,
    uint32_t logId)
    : windowSize_(windowSize),
      logDataQueue_(queueSize),
      // groupCommitQueue_(FLAGS_group_commit_queue_size),
      // writerThread_(nullptr),
      stopThread_(false),
      dataDirectory_(dataDirectory),
      logId_(logId),
      curQueue_(0),
      queueFlag_(-1),
      // numOfFlushedBuffers_(0),
      // numOfInsertedLogs_(0),

      // Checkpoint Log should be flushed to persist storage eagerly
      // logCheckpointQueue_(1),

      // One `allowedTimestampBehind` delay to allow the data to come in
      // and one more delay to allow the data to be dequeued and written.
      waitTimeBeforeClosing_(allowedTimestampBehind * 2),
      keepLogFilesAroundTime_(BucketUtils::duration(2, windowSize)) {
  
  CHECK_GT(windowSize, allowedTimestampBehind)
      << "Window size " << windowSize
      << " must be larger than allowedTimestampBehind "
      << allowedTimestampBehind;
  writerThreads_.resize(FLAGS_number_of_logging_queues);
  logDataQueues_.resize(FLAGS_number_of_logging_queues);
  shardWriters_.resize(FLAGS_number_of_logging_queues);

  queue_lock_.init();

  // for (int i = 0; i < FLAGS_group_commit_queue_size; ++i) {
  //   groupCommitQueue_.write(true);
  // }
  for (int i = 0; i < FLAGS_number_of_logging_queues; ++i) {
    logDataQueues_[i].reset(new folly::MPMCQueue<LogDataInfo>(queueSize));
  }
  startWriterThread();
}

BucketLogWriter::~BucketLogWriter() {
  stopWriterThread();
}

void BucketLogWriter::startWriterThread() {
  stopThread_ = false;
  if (FLAGS_number_of_logging_queues > 1) {
    // Initialize #FLAGS_number_of_logging_queues queues for current logging thread.
    for (int i = 0; i < FLAGS_number_of_logging_queues; ++i) {
      writerThreads_[i].reset(new std::thread([&, i]() {
        LOG(INFO) << "start !!!!!!!!!!! current thread ID: " << i << std::endl;
        // std::string threadName = "BucketLogWriter" + std::to_string(logId_) + std::to_string(i);
        prctl(PR_SET_NAME, "BucketLogWriter");
        
        while (true) {
          try {
            if (FLAGS_using_libpmem_or_libpmem2){
              if (!writeOneLogEntry_NVM(true, i)) {
                LOG(INFO) << "log thread " << i << " has aborted" << std::endl;
                break;
              }
            }
            else {
              if (!writeOneLogEntry(true)) {
                break;
              }
            }
          } catch (std::exception& e) {
            // Most likely a problem with filesystem.
            LOG(ERROR) << e.what();
            GorillaStatsManager::addStatValue(kLogFilesystemFailures, 1);
            usleep(kSleepUsBetweenFailures);
          }
        }
      }));
      LOG(INFO) << "start !!!!!!!!!!! current thread ID: " << i << std::endl;
    }
  }
  else {
    writerThreads_[0].reset(new std::thread([&]() {
      prctl(PR_SET_NAME,"BucketLogWriter");
      while (true) {
        try {
          if (FLAGS_using_libpmem_or_libpmem2){
            if (!writeOneLogEntry_NVM(true, 0)) {
              LOG(INFO) << "log thread " << logId_ << " has aborted" << std::endl;
              break;
            }
          }
          else {
            if (!writeOneLogEntry(true)) {
              break;
            }
          }
        } catch (std::exception& e) {
          // Most likely a problem with filesystem.
          LOG(ERROR) << e.what();
          GorillaStatsManager::addStatValue(kLogFilesystemFailures, 1);
          usleep(kSleepUsBetweenFailures);
        }
      }
    }));
  }
  
}

void BucketLogWriter::stopWriterThread() {
  if (FLAGS_number_of_logging_queues > 1) {
    for (int i = 0; i < FLAGS_number_of_logging_queues; ++i) {
      if (writerThreads_[i]) {
      stopThread_ = true;

      // Wake up and stop the writer thread. Just sends zeros to do
      // that.
      logData(0, kNoOpIndex, 0, 0);
      writerThreads_[i]->join();
    }
    }
  }
  else {
    if (writerThreads_[0]) {
      stopThread_ = true;

      // Wake up and stop the writer thread. Just sends zeros to do
      // that.
      logData(0, kNoOpIndex, 0, 0);
      writerThreads_[0]->join();
    }
  }
}

uint32_t BucketLogWriter::bucket(uint64_t unixTime, int shardId) const {
  return BucketUtils::bucket(unixTime, windowSize_, shardId);
}

uint64_t BucketLogWriter::timestamp(uint32_t bucket, int shardId) const {
  return BucketUtils::timestamp(bucket, windowSize_, shardId);
}

uint64_t BucketLogWriter::duration(uint32_t buckets) const {
  return BucketUtils::duration(buckets, windowSize_);
}

uint64_t BucketLogWriter::getRandomNextClearDuration() const {
  return random() % duration(1);
}

uint64_t BucketLogWriter::getRandomOpenNextDuration(int shardId) const {
  return windowSize_ * (0.75 + 0.25 * shardId / numShards_);
}

void BucketLogWriter::flushQueue() {
  stopWriterThread();
  if (FLAGS_number_of_logging_queues > 1) {
    for (int i = 0; i < FLAGS_number_of_logging_queues; ++i) {
      if (FLAGS_using_libpmem_or_libpmem2){
        while (writeOneLogEntry_NVM(false, i))
        ;
      }
      else {
        while (writeOneLogEntry(false))
        ;
      }
    }
  }
  else {
    if (FLAGS_using_libpmem_or_libpmem2){
      while (writeOneLogEntry_NVM(false, 0))
      ;
    }
    else {
      while (writeOneLogEntry(false))
      ;
    }
  }
  startWriterThread();
}

void BucketLogWriter::logData(
    int64_t shardId,
    int32_t index,
    int64_t unixTime,
    double value) {
  LogDataInfo info;
  info.shardId = shardId;
  info.index = index;
  info.unixTime = unixTime;
  info.value = value;

  logDataQueue_.blockingWrite(std::move(info));
  // if (!logDataQueue_.blockingWrite(std::move(info))) {
  //   GorillaStatsManager::addStatValue(kLogDataFailures, 1);
  // }
}

// This will log checkpoint to the persist storage directly.
void BucketLogWriter::logCheckpoint(int64_t shardId,
    int32_t index,
    int64_t unixTime,
    double value,
    int32_t flag) {
  LogDataInfo info;
  info.shardId = shardId;
  info.index = index;
  info.unixTime = unixTime;
  info.value = value;
  info.flag = flag;

  // Set the LSN as FLAGS_max_allowed_LSN to make it persist directly when flushing buffer.
  info.LSN = FLAGS_max_allowed_LSN + 1;
  
  int checkpointID = FLAGS_number_of_logging_queues;
  // Initialize the shardWriter for checkpoint
  if (shardWriters_.size() == FLAGS_number_of_logging_queues) {
    shardWriters_.push_back(std::unordered_map<int64_t, ShardWriter>());
  }

  auto iter = shardWriters_[checkpointID].find(info.shardId);
  if (iter == shardWriters_[checkpointID].end()) {
    // Randomly select the next clear time between windowSize_ and
    // windowSize_ * 2 to spread out the clear operations.
    ShardWriter writer;
    writer.nextClearTimeSecs =
        time(nullptr) + duration(1) + getRandomNextClearDuration();
    writer.fileUtils.reset(
        new FileUtils(info.shardId, kCheckpointPrefix, dataDirectory_));
    shardWriters_[checkpointID].insert(std::make_pair(info.shardId, std::move(writer)));
    // LOG(ERROR) << "The checkpoint is trying to write to a shard that is not enabled for "
    //               "writing "
    //             << info.shardId;
    // return;
  }
  
  iter = shardWriters_[checkpointID].find(info.shardId);
  int b = bucket(info.unixTime, info.shardId);
  ShardWriter& shardWriter = iter->second;
  auto& logWriter = shardWriter.logWriters[b];


  // If this bucket doesn't have a file open yet, open it now.
  if (!logWriter) {
    for (int i = 0; i < kFileOpenRetries; i++) {
      if (FLAGS_using_libpmem_or_libpmem2 == FLAGS_using_libpmem) {
        auto path = shardWriter.fileUtils->getPath(info.unixTime);
        LOG(INFO) << "Checkpoint - " << info.shardId << " open file!!!";
        logWriter.reset(new DataLogWriter(path.c_str(), info.unixTime, logId_, checkpointID));
        // LOG(INFO) << "LOG thread-" << threadID << " open file!!!";
        break;
      }
      // The following codes for libpmem2 have not finished.
      else{
        auto path = shardWriter.fileUtils->getPath(info.unixTime);
        auto fd = open(path.c_str(), O_RDWR);
        if (fd < 0) {
          PLOG(ERROR) << "Failed to open file: " << path;
        }
        else {
          logWriter.reset(new DataLogWriter(fd, info.unixTime));
          break;
        }
        if (i == kFileOpenRetries - 1) {
          LOG(ERROR) << "Failed too many times to open log file " << fd;
          GorillaStatsManager::addStatValue(kLogFilesystemFailures, 1);
        }
      }
      usleep(kSleepUsBetweenFailures);
    }
  }

  // LOG(INFO) << "Finish create data log object" << std::endl;
  // LOG(INFO) << "Insert a log entry, LSN = " << info.LSN << std::endl;

  if (logWriter) {
    // Convert the checkpoint log entry into bits without compaction.
    folly::fbstring bits;
    bits.reserve(28);
    addLogToBits(info, bits);
    // And record the checkpoint log. Flush the log entry directly.
    logWriter->flushBuffer(FLAGS_number_of_logging_queues, info.LSN, bits);

    // Logs in the buffer should be persisted before the checkpoint finished.
    // The following code is not used for now.
    if (info.index == DataLogReader::kStartCheckpoint) {
      time_t beginTime = time(nullptr);
      time_t endTime = time(nullptr);

      // Wait for logs with LSN > 0 persisted.
      while (snapshot_.numbers[logId_] !=0 && endTime - beginTime < kWaiteForLogsPersist) {
        endTime = time(nullptr);
      }

      // Initialize the compaction restart control bit.
      uint32_t temp = 0;
      folly::fbstring paddingBits;
      BitUtil::addValueToBitString(DataLogReader::kRestartCompaction, 4, paddingBits, temp);
      BitUtil::addValueToBitString(0, 4, paddingBits, temp);


      FileUtils files(
          info.shardId,
          kLogFilePrefix,
          *(getDataDirectory()));
      // There is no more insertion logs to fill up the log buffer.
      // Then we force the buffer to be flushed.
      // if (endTime - beginTime >= kWaiteForLogsPersist) {
      

      auto ids = files.ls();

      folly::MPMCQueue<std::pair<int64_t, LogDataInfo>> logAddrQueue(ids.size() * FLAGS_number_of_logging_queues);

      #pragma omp parallel
      #pragma omp for
      for (int i = 0; i < ids.size(); ++i) {
        auto id = ids[i];
        for (int curQueue = 0; curQueue < FLAGS_number_of_logging_queues; ++curQueue) {
          auto curIter = shardWriters_[curQueue].find(info.shardId);
          if (curIter == shardWriters_[curQueue].end()) {
            LOG(ERROR) << "Trying to write to a shard that is not enabled for "
                          "writing "
                      << info.shardId;
            continue;
          }

          ShardWriter& curShardWriter = curIter->second;
          
          int b = bucket(id / 10, info.shardId);
          auto curLogWriter = curShardWriter.logWriters.find(b);
          if (curLogWriter != curShardWriter.logWriters.end()) {
            curLogWriter->second->flushBuffer(curQueue, info.LSN, paddingBits);
            // The previous values should not be reset more than one time. 
            if (curQueue == 0) {
              curLogWriter->second->resetPreviousValues();
            }

            // Record the offset and the compaction values of the current active log file.
            LogDataInfo logAddr;
            logAddr.unixTime = curLogWriter->second->getLastTimestamp();
            logAddr.shardId = info.shardId;
            logAddr.index = DataLogReader::kLogFileOffset;
            logAddr.flag = FLAGS_log_flag_log_file_offset;
            // The value is used to record the offset.
            logAddr.value = curLogWriter->second->getCurPMemAddr() - curLogWriter->second->getPMemAddr();

            logAddr.LSN = FLAGS_max_allowed_LSN + 1;
            logAddrQueue.blockingWrite(std::make_pair(id, std::move(logAddr)));
          }
          else {
              LOG(ERROR) << "The log file " << id << " has been removed!" << std::endl;
          }
        }
      }

      // Reorder the log file offsets by id and append to the offsetBits.
      std::map<int64_t, LogDataInfo> logAddrRecords;
      std::pair<int64_t, LogDataInfo> elem;
      while(logAddrQueue.read(elem)) {
        logAddrRecords[elem.first] = elem.second;
      }

      // Convert the log entry into bits without compaction.
      folly::fbstring offsetBits;
      for (auto it = logAddrRecords.begin(); it != logAddrRecords.end(); ++it) {
        addLogToBits(std::move(it->second), offsetBits);
      }

      // Flush the checkpoint log entries directly.
      logWriter->flushBuffer(FLAGS_number_of_logging_queues, FLAGS_max_allowed_LSN + 1, offsetBits);
    }
  } else {
    GorillaStatsManager::addStatValue(kLogDataFailures, 1);
  }

  // Clear previous checkpoints.
  if(info.index == DataLogReader::kStopCheckpoint) {
    for (auto it = shardWriter.logWriters.begin(); it != shardWriter.logWriters.end() ; ++it) {
      if (it->first != b) {
        shardWriter.logWriters.erase(it->first);
      }
    }
    // Remove the checkpoint log files.
    shardWriter.fileUtils->clearTo(info.unixTime - keepLogFilesAroundTime_);
  }

}

void BucketLogWriter::addLogToBits(const LogDataInfo& info, folly::fbstring& bits) {
  uint32_t numBits = 0;
  BitUtil::addValueToBitString(info.index, 32, bits, numBits);
  BitUtil::addValueToBitString(info.unixTime, 64, bits, numBits);
  BitUtil::addValueToBitString((uint64_t)(info.value), 64, bits, numBits);
  BitUtil::addValueToBitString(info.flag, 32, bits, numBits);
  BitUtil::addValueToBitString(info.LSN, 32, bits, numBits);
}

int32_t BucketLogWriter::logData(
    int64_t shardId,
    int32_t index,
    int64_t unixTime,
    double value,
    int32_t flag,
    int64_t oriTime) {
  if (FLAGS_no_logging) {
    return 0;
  }
  LogDataInfo info;
  info.shardId = shardId;
  info.index = index;
  info.flag = flag;
  info.unixTime = unixTime;
  info.value = value;
  if (FLAGS_Beringei_NVM) {
    // This maybe unnecessary for Beringei.
    // The computeLSN() call is just used for test.
    info.LSN = computeLSN(flag);
  }
  else {
    info.LSN = computeLSN(flag, oriTime);
  }

  if (flag != FLAGS_log_flag_insertion && info.LSN == 0) {
    info.LSN = 0;
  }

  // info.LSN = 1;
  // To simulate update operations. 
  // info.LSN = float(std::rand())/RAND_MAX < 0.9 ? 0 : 1;

  logDataQueue_.blockingWrite(std::move(info));
  
  if (info.LSN != 0 ) {
    // if (info.LSN == 16) {
    // LOG(INFO) << "The LSN is: " << info.LSN << std::endl;
    // }

    // uint32_t curQueue;

    {
      folly::MSLGuard guard(queue_lock_);
      // std::unique_lock<std::mutex> nextQueueGuard(queue_mutex_);
      // curQueue = curQueue_;
      // nextQueue();
      // Update the number of logs with LSN larger than 0 for the current logging thread.
      snapshot_.numbers[logId_] += 1;

      //Update maximum LSN for the current logging thread.
      if (snapshot_.maxLSNs[logId_] > info.LSN) {
        snapshot_.maxLSNs[logId_] = info.LSN;
      }
      // LOG(INFO) << curQueue << std::endl;
    }

    // Enqueue a log entry, blocking until space is available. 
    // logDataQueues_[curQueue]->blockingWrite(std::move(info));
    
    // // Update the number of logs with LSN larger than 0 for the current logging thread.
    // snapshot_.numbers[logId_] += 1;

    // //Update maximum LSN for the current logging thread.
    // if (snapshot_.maxLSNs[logId_] > info.LSN) {
    //   snapshot_.maxLSNs[logId_] = info.LSN;
    // }
    // nextQueue();
  }
  // else {
  //   // Enqueue a log entry, blocking until space is available. 
  //   logDataQueues_[curQueue_]->blockingWrite(std::move(info));
  // }
  
  // LOG(INFO) << "Current waiting operation is " << numOfFlushedBuffers_ << std::endl;
  // Block until the logs have persisted.
  // groupCommitQueue_.blockingWrite(true);

  // LOG(INFO) << "One operation has returned." << std::endl;
  
  // The following codes are used for group commit.
  // However, the waiting thread can only be as many as the FLAGS_num_thrift_pool_threads.
  // And FLAGS_num_thrift_pool_threads * #eachLogSize may be much more smaller than FLAGS_data_log_buffer_size.
  // So that the following codes can only run when FLAGS_num_thrift_pool_threads >= 128, FLAGS_data_log_buffer_size =256,
  // and FLAGS_shards = 4.
  // Other settings may result in deadlock!!!!
  // For long transactions, the #logSize is much more larger than that of just one operation.
  // Then the following codes may be work for larger FLAGS_data_log_buffer_size.
  // {
  //   int curNumOfInsertedLogs = ++numOfInsertedLogs_;
  //   // LOG(INFO) << "Current waiting operation is " << numOfInsertedLogs_ << std::endl;
  //   std::unique_lock<std::mutex> commitGuard(commitMutex_);
  //   while (curNumOfInsertedLogs - numOfFlushedBuffers_ > 0) {
  //     commitCon_.wait(commitGuard);
  //   }
  //   // LOG(INFO) << "One operation has returned." << std::endl;
  // }
  
  return info.LSN;
}

void BucketLogWriter::nextQueue() {
  curQueue_ = curQueue_ + 1 == FLAGS_number_of_logging_queues ? 0 : curQueue_ + 1;
  // LOG(INFO) << "Current queue id is: " << curQueue_ << std::endl;
}

uint32_t BucketLogWriter::computeLSN(int32_t flag, int64_t oriTime) {
  // The LSN of inserted data is 0.
  if (flag == FLAGS_log_flag_insertion) {
    return 0;
  }

  //The following code compute the LSN of deleted and update Data.

  // Get the extention of timestamp,
  // oriUnixTime logic shift right unixTimesBits, then bitwise-and 0x3FF
  uint32_t oriLSN = (oriTime >> FLAGS_unix_time_bits) & 0x3FF;
  uint32_t newLSN = oriLSN + 1;

  // if(newLSN > 10) {
  //   LOG(INFO) << "The LSN is: " << newLSN << std::endl;
  // }
  

  if (!FLAGS_is_checkpoint && newLSN >= 1024){
    // call checkpoint procedure
    // checkpoint()
    LOG(ERROR) << "The LSN is: " << newLSN << std::endl;
    // This is just used for performance test.
    newLSN = 1;
  }
  return newLSN;
}

// WARNING: THIS FUNCTION IS JUST USED FOR TEST.
uint32_t BucketLogWriter::computeLSN(int32_t flag) {
  // if (flag == FLAGS_log_flag_insertion) {
  //   return 0;
  // }
  uint32_t newLSN;
  g_mutex.lock();
  // if (facebook::gorilla::gLSN == UINT32_MAX){
  //   // call checkpoint procedure
  //   // checkpoint()
  //   facebook::gorilla::gLSN = 0;
  // }
  if (flag == FLAGS_log_flag_insertion) {
    newLSN = 0;
  }
  newLSN = ++gLSN;

  // Update gLSN to avoid data overflow.
  gLSN = (gLSN > FLAGS_max_allowed_LSN ? 0 : gLSN);
  // newLSN = facebook::gorilla::gLSN;
  g_mutex.unlock();
  // return 0;
  return newLSN;
}

bool BucketLogWriter::writeOneLogEntry(bool blockingRead) {
  // This code assumes that there's only a single thread running here!
  std::vector<LogDataInfo> data;
  LogDataInfo info;

  if (stopThread_ && blockingRead) {
    return false;
  }

  if (blockingRead) {
    // First read is blocking then as many as possible without blocking.
    logDataQueue_.blockingRead(info);
    data.push_back(std::move(info));
  }

  while (logDataQueue_.read(info)) {
    data.push_back(std::move(info));
  }

  bool onePreviousLogWriterCleared = false;

  for (const auto& info : data) {
    if (info.index == kStartShardIndex) {
      ShardWriter writer;

      // Randomly select the next clear time between windowSize_ and
      // windowSize_ * 2 to spread out the clear operations.
      writer.nextClearTimeSecs =
          time(nullptr) + duration(1) + getRandomNextClearDuration();
      writer.fileUtils.reset(
          new FileUtils(info.shardId, kLogFilePrefix, dataDirectory_));
      shardWriters_[0].insert(std::make_pair(info.shardId, std::move(writer)));
    } else if (info.index == kStopShardIndex) {
      LOG(INFO) << "Stopping shard " << info.shardId;
      shardWriters_[0].erase(info.shardId);
    } else if (info.index != kNoOpIndex) {
      auto iter = shardWriters_[0].find(info.shardId);
      if (iter == shardWriters_[0].end()) {
        LOG(ERROR) << "Trying to write to a shard that is not enabled for "
                      "writing "
                   << info.shardId;
        continue;
      }

      int b = bucket(info.unixTime, info.shardId);
      ShardWriter& shardWriter = iter->second;
      auto& logWriter = shardWriter.logWriters[b];

      // If this bucket doesn't have a file open yet, open it now.
      if (!logWriter) {
        for (int i = 0; i < kFileOpenRetries; i++) {
          auto f = shardWriter.fileUtils->open(
              info.unixTime, "wb", kLogFileBufferSize);
          if (f.file) {
            logWriter.reset(new DataLogWriter(std::move(f), info.unixTime));
            break;
          }
          if (i == kFileOpenRetries - 1) {
            LOG(ERROR) << "Failed too many times to open log file " << f.name;
            GorillaStatsManager::addStatValue(kLogFilesystemFailures, 1);
          }
          usleep(kSleepUsBetweenFailures);
        }
      }

      // Spread out opening the files for the next bucket in the last
      // 1/4 of the time window based on the shard ID. This will avoid
      // opening a lot of files simultaneously.
      uint32_t openNextFileTime =
          timestamp(b, info.shardId) + getRandomOpenNextDuration(info.shardId);
      if (time(nullptr) > openNextFileTime &&
          shardWriter.logWriters.find(b + 1) == shardWriter.logWriters.end()) {
        uint32_t baseTime = timestamp(b + 1, info.shardId);
        LOG(INFO) << "Opening file in advance for shard " << info.shardId;
        for (int i = 0; i < kFileOpenRetries; i++) {
          auto f =
            shardWriter.fileUtils->open(baseTime, "wb", kLogFileBufferSize);
          if (f.file) {
            // shardWriter.logWriters[b + 1].resize(1);
            shardWriter.logWriters[b + 1].reset(
                new DataLogWriter(std::move(f), baseTime));
            break;
          }

          if (i == kFileOpenRetries - 1) {
            // This is kind of ok. We'll try again above.
            LOG(ERROR) << "Failed too many times to open log file " << f.name;
          }
          usleep(kSleepUsBetweenFailures);
        }
      }

      // Only clear at most one previous bucket because the operation
      // is really slow and queue might fill up if multiple buckets
      // are cleared.

      auto now = time(nullptr);
      if (!onePreviousLogWriterCleared &&
          now - BucketUtils::floorTimestamp(now, windowSize_, info.shardId) >
              waitTimeBeforeClosing_ &&
          shardWriter.logWriters.find(b - 1) != shardWriter.logWriters.end()) {
        shardWriter.logWriters.erase(b - 1);
        onePreviousLogWriterCleared = true;
      }

      if (now > shardWriter.nextClearTimeSecs) {
        shardWriter.fileUtils->clearTo(time(nullptr) - keepLogFilesAroundTime_);
        shardWriter.nextClearTimeSecs += duration(1);
      }

      if (logWriter) {
        logWriter->append(info.index, info.unixTime, info.value);
      } else {
        GorillaStatsManager::addStatValue(kLogDataFailures, 1);
      }
    }
  }

  // Don't flush any of the logWriters. DataLog class will handle the
  // flushing when there's enough data.
  // return !data.empty();
  return false;
}

bool BucketLogWriter::writeOneLogEntry_NVM(bool blockingRead, const int threadID) {
  // folly::MSLGuard guard(queue_lock_);

  // std::unique_lock<std::mutex> nextQueueGuard(queue_mutex_);
  // This code assumes that there's only a single thread running here!
  std::vector<LogDataInfo> data;
  {
    std::unique_lock<std::mutex> conGuard(queue_mutex_);
        
    // LOG(INFO) << "thread id: " << std::this_thread::get_id() << std::endl;

    // Wait until the other thread have flushed the log with LSN > 0.
    if (threadID - queueFlag_ != 1 && queueFlag_ - threadID != FLAGS_number_of_logging_queues - 1){
      // DataLogWriter::loggingCon_[0].notify_all();
      queueCon_.wait(conGuard);
    }

    bool isUpdateLog = false;
    // LOG(INFO) << "current thread ID: " << threadID << ", logID:" << logId_ << std::endl;

    
    LogDataInfo info;

    if (stopThread_ && blockingRead) {
      return false;
    }

    if (blockingRead) {
      // First read is blocking then as many as possible without blocking.
      // logDataQueues_[threadID]->blockingRead(info);
      logDataQueue_.blockingRead(info);
      data.emplace_back(std::move(info));
      if (info.flag == FLAGS_log_flag_deletion || info.flag == FLAGS_log_flag_update) {
        isUpdateLog = true;
        queueFlag_ = threadID;
        queueCon_.notify_all();
      }
    }

    while (!isUpdateLog && logDataQueue_.read(info)) {
      data.emplace_back(std::move(info));
      if (info.flag == FLAGS_log_flag_deletion || info.flag == FLAGS_log_flag_update) {
        isUpdateLog = true;
        queueFlag_ = threadID;
        queueCon_.notify_all();
        break;
      }
    }
  }

  bool onePreviousLogWriterCleared = false;

  for (const auto& info : data) {
    // LOG(INFO) << "current thread ID: " << threadID << ", logID:" << logId_ << std::endl;
    // LOG(INFO) << "timestamp is " << info.unixTime;
    // LOG(INFO) << "LSN is " << info.LSN;
    if (info.index == kStartShardIndex) {
      LOG(INFO) << "start shard: " << info.shardId << std::endl;
      // The thread of logging queue 0 is responsible to start new shards.
      for (int i = 0; i < FLAGS_number_of_logging_queues; ++i) {
        ShardWriter writer;
        // Randomly select the next clear time between windowSize_ and
        // windowSize_ * 2 to spread out the clear operations.
        writer.nextClearTimeSecs =
            time(nullptr) + duration(1) + getRandomNextClearDuration();
        writer.fileUtils.reset(
            new FileUtils(info.shardId, kLogFilePrefix, dataDirectory_));
        shardWriters_[i].insert(std::make_pair(info.shardId, std::move(writer)));
      }
      
    } else if (info.index == kStopShardIndex) {
      LOG(INFO) << "Stopping shard " << info.shardId;
      // The thread of logging queue 0 is responsible to stop obsolete shards.
      for (int i = 0; i < FLAGS_number_of_logging_queues; ++i) {
        shardWriters_[i].erase(info.shardId);
      }
    } else if (info.index != kNoOpIndex) {
      auto iter = shardWriters_[threadID].find(info.shardId);
      if (iter == shardWriters_[threadID].end()) {
        LOG(ERROR) << "Trying to write to a shard that is not enabled for "
                      "writing "
                   << info.shardId;
        continue;
      }

      int b = bucket(info.unixTime, info.shardId);
      
      if (info.LSN == 0) {
        b = bucket(info.unixTime, info.shardId);
        // Update latestBucket_.
        latestBucket_ = b == latestBucket_ ? latestBucket_ + 1 : latestBucket_;
      }
      // The updated Logs are forced to record into the latest active bucket.
      else {
        b = latestBucket_ - 1;
      }

      ShardWriter& shardWriter = iter->second;
      auto& logWriter = shardWriter.logWriters[b];

      // If this bucket doesn't have a file open yet, open it now.
      if (!logWriter) {
        for (int i = 0; i < kFileOpenRetries; i++) {
          if (FLAGS_using_libpmem_or_libpmem2 == FLAGS_using_libpmem) {
            auto path = shardWriter.fileUtils->getPath(
            info.unixTime, threadID);
            LOG(INFO) << "LOG thread-" << threadID << " open file!!!";
            logWriter.reset(new DataLogWriter(path.c_str(), info.unixTime, logId_, threadID));
            // LOG(INFO) << "LOG thread-" << threadID << " open file!!!";
            break;
          }
          // The following codes for libpmem2 have not finished.
          else{
            auto path = shardWriter.fileUtils->getPath(info.unixTime, threadID);
            auto fd = open(path.c_str(), O_RDWR);
            if (fd < 0) {
              PLOG(ERROR) << "Failed to open file: " << path;
            }
            else {
              logWriter.reset(new DataLogWriter(fd, info.unixTime));
              break;
            }
            if (i == kFileOpenRetries - 1) {
              LOG(ERROR) << "Failed too many times to open log file " << fd;
              GorillaStatsManager::addStatValue(kLogFilesystemFailures, 1);
            }
          }
          usleep(kSleepUsBetweenFailures);
        }
      }

      // LOG(INFO) << "Finish create data log object" << std::endl;

      // Spread out opening the files for the next bucket in the last
      // 1/4 of the time window based on the shard ID. This will avoid
      // opening a lot of files simultaneously.
      uint32_t openNextFileTime =
          timestamp(b, info.shardId) + getRandomOpenNextDuration(info.shardId);
      if (time(nullptr) > openNextFileTime && shardWriter.logWriters.find(b + 1) == shardWriter.logWriters.end()){
        uint32_t baseTime = timestamp(b + 1, info.shardId);
        for (int i = 0; i < kFileOpenRetries; i++) {
          if (FLAGS_using_libpmem_or_libpmem2 == FLAGS_using_libpmem) {
            auto path = shardWriter.fileUtils->getPath(baseTime, threadID);
            LOG(INFO) << "LOG thread-" << logId_ << ", queue-" << threadID << " open file!!!";
            // LOG(INFO) << "Finish create data log object for the next bucket, path" << path.c_str() << std::endl;
            shardWriter.logWriters[b + 1].reset(new DataLogWriter(path.c_str(), baseTime, logId_, threadID));
            // LOG(INFO) << "Finish create data log object for the next bucket" << std::endl;
            break;
          }
          // The following codes for libpmem2 have not finished.
          else{
            auto path = shardWriter.fileUtils->getPath(baseTime, threadID);
            auto fd = open(path.c_str(), O_RDWR);
            if (fd < 0) {
              PLOG(ERROR) << "Failed to open file: " << path;
            }
            else {
              shardWriter.logWriters[b + 1].reset(
                  new DataLogWriter(fd, baseTime));
              break;
            }

            if (i == kFileOpenRetries - 1) {
              // This is kind of ok. We'll try again above.
              LOG(ERROR) << "Failed too many times to open log file " << baseTime;
            }
            usleep(kSleepUsBetweenFailures);
          }
        }
        latestBucket_ = b + 1;
      }

      // Only clear at most one previous bucket because the operation
      // is really slow and queue might fill up if multiple buckets
      // are cleared.

      auto now = time(nullptr);
      if (!onePreviousLogWriterCleared &&
          now - BucketUtils::floorTimestamp(now, windowSize_, info.shardId) >
              waitTimeBeforeClosing_ &&
          shardWriter.logWriters.find(b - 1) != shardWriter.logWriters.end()) {
        LOG(INFO) << "Erase bucket!!!!!!: " << b - 1 << std::endl;
        shardWriter.logWriters.erase(b - 1);
        onePreviousLogWriterCleared = true;
      }

      if (now > shardWriter.nextClearTimeSecs) {
        LOG(INFO) << "Clear files!!!!!!!" << std::endl;
        shardWriter.fileUtils->clearTo(time(nullptr) - keepLogFilesAroundTime_);
        shardWriter.nextClearTimeSecs += duration(1);
      }

      // LOG(INFO) << "Insert a log entry, LSN = " << info.LSN << std::endl;

      if (logWriter) {
        uint32_t numOfLogs = 0;
        // if (!isUpdateLog && (info.flag == FLAGS_log_flag_update || info.flag == FLAGS_log_flag_deletion)) {
        //   queueCon_.notify_all();
        // }
        
        if (logWriter->append(info.index, info.unixTime, info.value, info.flag, info.LSN, numOfLogs)) {
          // LOG(INFO) << numOfLogs <<" logs have been flushed!!!!!! " << std::endl;
          // bool temp;
          // for (int i = 0; i < numOfLogs; ++i) {
          //   groupCommitQueue_.blockingRead(temp);
          // }

          // // LOG(INFO) << numOfLogs <<" logs in groupCommitQueue have been read!!!!!! " << std::endl;
          // std::unique_lock<std::mutex> flushGuard(commitMutex_);
          // numOfFlushedBuffers_.fetch_add(numOfLogs, std::memory_order_relaxed); 
          // // // LOG(INFO) << "numOfFlushedBuffers = " << numOfFlushedBuffers << std::endl;
          // // if (numOfFlushedBuffers == FLAGS_group_commit_numbers) {
          // commitCon_.notify_all();
          // }
        }
      } else {
        GorillaStatsManager::addStatValue(kLogDataFailures, 1);
      }
    }
  }

  
  // Don't flush any of the logWriters. DataLog class will handle the
  // flushing when there's enough data.
  // if (data.empty())
  return !data.empty();
}

void BucketLogWriter::startShard(int64_t shardId) {
  LogDataInfo info;
  info.shardId = shardId;
  info.index = kStartShardIndex;
  // logDataQueues_[0]->blockingWrite(std::move(info));
  logDataQueue_.blockingWrite(std::move(info));
}

void BucketLogWriter::stopShard(int64_t shardId) {
  LogDataInfo info;
  info.shardId = shardId;
  info.index = kStopShardIndex;
  // logDataQueues_[0]->blockingWrite(std::move(info));
  logDataQueue_.blockingWrite(std::move(info));
}

void BucketLogWriter::startMonitoring() {
  GorillaStatsManager::addStatExportType(kLogDataFailures, SUM);
  GorillaStatsManager::addStatExportType(kLogFilesystemFailures, SUM);
}
}
} // facebook:gorilla

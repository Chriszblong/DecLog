/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <memory>
#include <thread>
#include <unordered_map>

#include "PersistentKeyList.h"

#include <folly/MPMCQueue.h>

namespace facebook {
namespace gorilla {

class KeyListWriter {
 public:
  static const std::string kLogFilePrefix;

  KeyListWriter(const std::string& dataDirectory, size_t queueSize);

  ~KeyListWriter();

  // Copy a new key onto the queue for writing.
  void addKey(
      int64_t shardId,
      uint32_t id,
      const std::string& key,
      uint16_t category);

  // Pass a compaction call down to the appropriate PersistentKeyList.
  void compact(
      int64_t shardId,
      std::function<std::tuple<uint32_t, const char*, uint16_t>()> generator);

  void startShard(int64_t shardId);
  void stopShard(int64_t shardId);

  static void startMonitoring();

  void flushQueue();

  const std::string* getDataDirectory(){
    return &dataDirectory_;
  } 

 private:
  std::shared_ptr<PersistentKeyList> get(int64_t shardId);
  void enable(int64_t shardId);
  void disable(int64_t shardId);

  void stopWriterThread();
  void startWriterThread();

  // Write a single entry from the queue.
  bool writeOneKey();

  struct KeyInfo {
    int64_t shardId;
    std::string key;
    int32_t keyId;
    enum { STOP_THREAD, START_SHARD, STOP_SHARD, WRITE_KEY } type;
    uint16_t category;
  };

  folly::MPMCQueue<KeyInfo> keyInfoQueue_;
  std::unique_ptr<std::thread> writerThread_;
  const std::string dataDirectory_;

  std::mutex lock_;
  std::unordered_map<int64_t, std::shared_ptr<PersistentKeyList>> keyWriters_;
};
}
} // facebook:gorilla

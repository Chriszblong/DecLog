/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <folly/SharedMutex.h>
#include <folly/experimental/FunctionScheduler.h>
#include <mutex>

#include "beringei/client/BeringeiConfigurationAdapterIf.h"
#include "beringei/if/gen-cpp2/BeringeiService.h"
#include "beringei/lib/BucketMap.h"
#include "beringei/lib/MemoryUsageGuardIf.h"
#include "beringei/lib/ShardData.h"

/* using override */
using facebook::gorilla::BeringeiServiceSvIf;

class BeringeiServiceHandlerTest;

namespace facebook {
namespace gorilla {

class BeringeiServiceHandler : virtual public BeringeiServiceSvIf {
  friend class ::BeringeiServiceHandlerTest;

 public:
  static const int kAsyncDropShardsDelaySecs;

  BeringeiServiceHandler(
      std::shared_ptr<BeringeiConfigurationAdapterIf> configAdapter,
      std::shared_ptr<MemoryUsageGuardIf> memoryUsageGuard,
      const std::string& serviceName,
      int port);

  virtual ~BeringeiServiceHandler();

  virtual void getData(GetDataResult& ret, std::unique_ptr<GetDataRequest> req)
      override;

  // written by yls
  // 读取扩展时间戳
  bool getExtension(
      const Key& key,
      int64_t timeBegin,
      int64_t timeEnd,
      std::vector<std::pair<int64_t, uint64_t>>& out);

  virtual void putDataPoints(
      PutDataResult& response,
      std::unique_ptr<PutDataRequest> req) override;

  virtual void updateDataPoints(
      UpdateDataPointsResult& ret,
      std::unique_ptr<UpdateDataPointsRequest> req)
      override; // override强制要求函数是虚函数，否则编译错误

  virtual void deleteDataPoints(
      DeleteDataPointsResult& ret,
      std::unique_ptr<DeleteDataPointsRequest> req) override;

  virtual void getShardDataBucket(
      GetShardDataBucketResult& ret,
      int64_t beginTs,
      int64_t endTs,
      int64_t shardId,
      int32_t offset,
      int32_t limit) override;

  virtual BucketMap* getShardMap(int64_t shardId);

  virtual void getLastUpdateTimes(
      GetLastUpdateTimesResult& ret,
      std::unique_ptr<GetLastUpdateTimesRequest> req) override;

  // written by yls
  // 提交事务
  void doTransaction(TxResult& ret, std::unique_ptr<TxRequest> req);

  // written by yls
  // 为事务中的每个操作访问的数据上锁
  bool lockDataPointsInOp(std::vector<TxRequestOp>& op);

  // written by yls
  // 为给定时序的多个区间内数据解锁
  bool unlockDataPointsInOp(
      const Key& key,
      std::vector<std::pair<int64_t, int64_t>>& intervals);

  // written by yls
  // 为事务中的每个操作访问的数据解锁
  bool unlockDataPointsInOp(std::vector<TxRequestOp>& op);

  // written by yls
  // 合并区间
  static void mergeInterval(
      std::vector<std::pair<int64_t, int64_t>>& intervals,
      std::vector<std::pair<int64_t, int64_t>>& ret);

  void checkpoint();

  void purgeThread();
  void cleanThread();

  // Purges time series that have no data in the active bucket and not
  // in any of the `numBuckets` older buckets.
  int purgeTimeSeries(uint8_t numBuckets);

  void finalizeBucket(const uint64_t timestamp);
  void finalizeBucketsThread();

 private:
  // Reads shard map (via configAdapter_) to learn of shards that have been
  // added or dropped.  Invoked periodically via function scheduler.
  void refreshShardConfig();

  ShardData shards_;

  std::shared_ptr<BeringeiConfigurationAdapterIf> configAdapter_;
  std::shared_ptr<MemoryUsageGuardIf> memoryUsageGuard_;
  const std::string serviceName_;
  const int32_t port_;

  folly::FunctionScheduler purgeThread_;
  folly::FunctionScheduler cleanThread_;
  folly::FunctionScheduler bucketFinalizerThread_;
  folly::FunctionScheduler refreshShardConfigThread_;
  std::unique_ptr<std::thread> checkpointThread;

  std::mutex checkpointMutex_;
  pthread_rwlock_t insertTxRwlock_;
  pthread_rwlock_t updateTxRwlock_;
  std::condition_variable checkpointReadyCv_;
  std::condition_variable checkpointCompleteCv_;
  bool requireCheckpoint;
};

// written by yls
// Key的hash函数
struct KeyHash {
  std::size_t operator()(Key const& key) const noexcept {
    std::size_t h1 = std::hash<std::string>{}(key.get_key());
    std::size_t h2 = std::hash<int64_t>{}(key.get_shardId());
    return h1 ^ (h2 << 1);
  }
};

} // namespace gorilla
} // namespace facebook

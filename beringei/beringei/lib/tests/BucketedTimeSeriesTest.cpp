/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <tuple>

#include "beringei/if/gen-cpp2/beringei_data_types.h"
#include "beringei/lib/BucketStorage.h"
#include "beringei/lib/BucketedTimeSeries.h"
#include "beringei/lib/TimeSeries.h"

using namespace ::testing;
using namespace facebook::gorilla;
using namespace std;

typedef vector<pair<uint8_t, vector<TimeValuePair>>> In;

template <class T>
using Out = vector<tuple<uint8_t, uint8_t, T>>;

typedef vector<TimeSeriesBlock> Block;

template <class Bucket>
void test(
    Bucket& buckets,
    BucketStorage* storage,
    const In& in,
    const Out<typename Bucket::Output>& out) {

  int64_t beginTime = 0;
  int64_t endTime = 0;
  // Insert data.
  for (auto& bucket : in) {
    for (auto& value : bucket.second) {
      buckets.put(bucket.first, value, storage, 0, nullptr);
      if (beginTime == 0){
        beginTime = value.unixTime;
      }
      endTime = value.unixTime > endTime ? value.unixTime : endTime;
    }
  }

  // Read it back out.
  for (auto& query : out) {
    typename Bucket::Output output;
    buckets.get(beginTime, endTime, get<0>(query), get<1>(query), output, storage);
    auto& base = get<2>(query);
    ASSERT_EQ(base.size(), output.size());
    for (int i = 0; i < base.size(); i++) {
      EXPECT_EQ(base[i].count, output[i].count);
      EXPECT_EQ(base[i], output[i]);
    }
  }
}

class BucketedTimeSeriesTest : public testing::Test {
 protected:
  void SetUp() {
    tv[0].unixTime = 60;
    tv[0].value = 0.0;
    tv[1].unixTime = 120;
    tv[1].value = 2.5;
    tv[2].unixTime = 180;
    tv[2].value = 5.0;
    tv[3].unixTime = 240;
    tv[3].value = 7.5;
    tv[4].unixTime = 300;
    tv[4].value = 10.0;

    blocks.emplace_back();
    TimeSeries::writeValues({tv[0], tv[1], tv[2]}, blocks.back());

    blocks.emplace_back();
    TimeSeries::writeValues({tv[3], tv[4]}, blocks.back());
  }

  In source0() {
    return {
        {7, {tv[0], tv[1], tv[2]}}, {8, {tv[3], tv[4]}},
    };
  }

  Out<vector<TimeSeriesBlock>> ts0() {
    return {
        make_tuple<uint32_t, uint32_t, Block>(3, 4, {}),
        make_tuple<uint32_t, uint32_t, Block>(0, 100, {blocks[0], blocks[1]}),
        make_tuple<uint32_t, uint32_t, Block>(8, 8, {blocks[1]})};
  }

  TimeValuePair tv[5];
  vector<TimeSeriesBlock> blocks;
};

TEST_F(BucketedTimeSeriesTest, TimeSeries) {
  BucketedTimeSeries bucket;
  bucket.reset(5);
  BucketStorage storage(5, 0, "");
  test<BucketedTimeSeries>(bucket, &storage, source0(), ts0());
}

TEST(BucketedTimeSeriesTest2, QueriedBucketsAgo) {
  BucketedTimeSeries bucket;
  bucket.reset(5);
  BucketStorage storage(5, 0, "");

  // No queries yet.
  ASSERT_EQ(255, bucket.getQueriedBucketsAgo());

  TimeValuePair value;
  value.unixTime = 10;
  value.value = 10;

  bucket.put(1, value, &storage, 12, nullptr);

  // Still no queries.
  ASSERT_EQ(255, bucket.getQueriedBucketsAgo());

  bucket.setQueried();

  // Was just queried.
  ASSERT_EQ(0, bucket.getQueriedBucketsAgo());
  bucket.put(2, value, &storage, 12, nullptr);

  // New bucket started after last get
  ASSERT_EQ(1, bucket.getQueriedBucketsAgo());
}

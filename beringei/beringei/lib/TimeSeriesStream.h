/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <stdint.h>
#include <limits>
#include <vector>

#include <folly/FBString.h>
#include <folly/Range.h>

#include "beringei/if/gen-cpp2/beringei_data_types.h"

#include "DataBlock.h"
#include "FileUtils.h"

namespace facebook {
namespace gorilla {


// This class operates on a stream of TimeSeries Data (timestamp, double value)
class TimeSeriesStream {
 public:
  TimeSeriesStream();
  void reset();

  // Size in bytes of the data.
  uint32_t size();

  // Allocated size in bytes of the data
  uint32_t capacity();

  // itemcount of the data.
  uint32_t count(){
    return count_;
  }

  //written by yls
  //序列化stream
  void finalize(uint32_t timeseriesId, FILE* f);

  size_t deserializeStream(const char* ptr);

  // Copy everything out into the provided buffer.
  void readData(char* out, uint32_t size);

  // Copy everything out into the provided string.
  void readData(std::string& out);

  // Returns the raw pointer to the stream data.
  const char* getDataPtr();

  // Appends a time value to the current stream. Returns true if the
  // value was successfully added, false otherwise. This function
  // might return false if it considers the value to be spam, i.e., it
  // was sent too soon after the previous value.
  bool append(const TimeValuePair& value, int64_t minTimestampDelta);

  // Same as above
  bool append(int64_t unixTime, double value, int64_t minTimestampDelta);

  // Extract the at most n values that are between begin and end
  // inclusive and put them in a vector. The value will be replaced when
  // the data point exists in the updatedDataMap. Assumes there are n values
  // in the series and space for n values in the vector. Returns the
  // number of values read.
  template <typename T>
  static int readValues(
      T& out,
      folly::StringPiece data,
      int n,
      int64_t begin = 0,
      int64_t end = std::numeric_limits<int64_t>::max(),
      const std::map<int64_t, double>* updatedDataMap = NULL);

  // The same, but use the data stored in `this`.
  template <typename T>
  int readValues(
      T& out,
      int n,
      int64_t begin = 0,
      int64_t end = std::numeric_limits<int64_t>::max()) {
    return readValues(out, data_, n, begin, end);
  }

    //written by yls
  //给stroage block内的某范围内数据点原地上锁
  static bool lockInExtension(
      char* block,
      uint16_t dataLength,
      uint16_t itemCount,
      int64_t begin,
      int64_t end);
  
  //written by yls
  //给stream block内的某范围内数据点原地上锁
  static bool lockInExtension(
      TimeSeriesStream& block,
      uint16_t dataLength,
      uint16_t itemCount,
      int64_t begin,
      int64_t end);


  //written by yls
  //给block内的某范围内数据点原地解锁
  static bool unlockInExtension(
      char* block,
      uint16_t dataLength,
      uint16_t itemCount,
      int64_t begin,
      int64_t end);

  //written by yls
  //给stream block内的某范围内数据点原地解锁
  static bool unlockInExtension(
      TimeSeriesStream& block,
      uint16_t dataLength,
      uint16_t itemCount,
      int64_t begin,
      int64_t end);

  //written by yls
  //将修改刷新到storage中
  static void flushUpdate(
      const char* data,
      uint16_t dataLength,
      uint16_t itemCount,
      std::map<int64_t, folly::fbstring> &updatedDataMap,
      TimeSeriesStream& block);


  // Extract the at most itemCount values and put them in a vector. 
  // Assumes there are itemCount values in the series and space 
  // for n values in the vector. 
  static void createUpdatedDataMap(
      folly::StringPiece data,
      int itemCount,
      std::map<int64_t, double>* updatedDataMap);

  uint32_t getPreviousTimeStamp() {
    return prevTimestamp_;
  }

  // uint64_t getFirstValue() {
  //   return firstValue_;
  // }

  uint32_t getFirstTimeStamp();

  // Find the data point from steam_.
  // If the data point exists, return the ID of lock bit
  // Otherwise, return -1.
  // int findDataPointInStream(TimeSeriesStream& block, const TimeValuePair& value, const uint16_t& itemCount);

  // Find the data point from steam_.
  // If the data point exists, return the ID of lock bit
  // Otherwise, return -1.
  static int64_t findDataPointInStream(
    const char* block, 
    const TimeValuePair& value, 
    const uint16_t& itemCount,
    int64_t& targetTimestamp);

  folly::fbstring data_;
  uint32_t count_;

 private:
  static constexpr uint32_t kLeadingZerosLengthBits = 5;
  static constexpr uint32_t kBlockSizeLengthBits = 6;
  static constexpr uint32_t kMaxLeadingZerosLength =
      (1 << kLeadingZerosLengthBits) - 1;
  static constexpr uint32_t kBlockSizeAdjustment = 1;
  static constexpr uint32_t kDefaultDelta = 60;
  static constexpr uint32_t kBitsForFirstTimestamp = 31; // Works until 2038.

  // Decompression methods.
  static double readNextValue(
      const char* data,
      uint64_t& bitPos,
      uint64_t& previousValue,
      uint64_t& previousLeadingZeros,
      uint64_t& previousTrailingZeros);
  static int64_t readNextTimestamp(
      const char* data,
      uint64_t& bitPos,
      int64_t& prevValue,
      int64_t& prevDelta);

  // Compression methods.
  bool appendTimestamp(int64_t timestamp, int64_t minTimestampDelta);

  void appendValue(double value);

  // Add one bit as a tombstone and one bit as a lock.
  void appendLockAndTombstone();

  
  uint64_t previousValue_;
  // uint64_t firstValue_;
  uint32_t numBits_;
  uint32_t prevTimestamp_;
  uint32_t prevTimestampDelta_;
  uint8_t previousValueLeadingZeros_;
  uint8_t previousValueTrailingZeros_;

 public:
  // 16 unused bits.
  uint16_t extraData;
};
}
} // facebook::gorilla

#include "TimeSeriesStream-inl.h"

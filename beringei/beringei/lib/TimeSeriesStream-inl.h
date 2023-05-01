/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <gflags/gflags.h>

#include "BitUtil.h"

DECLARE_int64(gorilla_blacklisted_time_min);
DECLARE_int64(gorilla_blacklisted_time_max);

namespace facebook {
namespace gorilla {

namespace {

template <typename T>
using is_vector = std::
    is_same<T, std::vector<typename T::value_type, typename T::allocator_type>>;

template <typename T>
inline typename std::enable_if<is_vector<T>::value>::type
addValueToOutput(T& out, int64_t unixTime, double value) {
  out.emplace_back();
  out.back().unixTime = unixTime;
  out.back().value = value;
}

template <typename T>
inline typename std::enable_if<!is_vector<T>::value>::type
addValueToOutput(T& out, int64_t unixTime, double value) {
  out[unixTime] = value;
}

// Call reserve() only if it exists.
template <typename T>
inline typename std::enable_if<
    std::is_member_function_pointer<decltype(&T::reserve)>::value>::type
reserve(T* out, size_t n) {
  out->reserve(n);
}

inline void reserve(...) {}

} // namespace

template <typename T>
int TimeSeriesStream::readValues(
    T& out,
    folly::StringPiece data,
    int n,
    int64_t begin,
    int64_t end,
    const std::map<int64_t, double>* updatedDataMap) {
  if (data.empty() || n == 0) {
    return 0;
  }

  reserve(&out, out.size() + n);

  uint64_t previousValue = 0;
  uint64_t previousLeadingZeros = 0;
  uint64_t previousTrailingZeros = 0;
  uint64_t bitPos = 0;
  int64_t previousTimestampDelta = kDefaultDelta;

  int64_t firstTimestamp = BitUtil::readValueFromBitString(
      data.data(), bitPos, kBitsForFirstTimestamp);
  double firstValue = readNextValue(
      data.data(),
      bitPos,
      previousValue,
      previousLeadingZeros,
      previousTrailingZeros);
  int64_t previousTimestamp = firstTimestamp;

  // Read the tombstone bit.
  uint64_t tombstone = BitUtil::readValueFromBitString(data.data(), bitPos, 1);
  // Read the lock bit which is useless for the query.
  BitUtil::readValueFromBitString(data.data(), bitPos, 1);

  // If the first data point is after the query range, return nothing.
  if (firstTimestamp > end) {
    return 0;
  }

  LOG(INFO) << "Timestamp is " << firstTimestamp << std::endl;
  LOG(INFO) << "The original value is " << firstValue << std::endl;
  LOG(INFO) << "The tombstone is " << tombstone << std::endl;

  std::map<int64_t, double>::const_iterator it = updatedDataMap->begin();
  int count = 0;
  if (firstTimestamp >= begin) {
    if (updatedDataMap && tombstone) {
      auto curIt = updatedDataMap->find(firstTimestamp);
      // Update the value if the data point exists in the updatedDataMap.
      if (curIt != updatedDataMap->end()) {
        firstValue = curIt->second;
        LOG(INFO) << "The newest value is " << firstValue << std::endl;
      }

      // Output data points which exist just in the updatedDataMap.
      // The code works when data points in block and updatedDataMap
      // are both in order.
      while(it != curIt && it != updatedDataMap->end()) {
        if (it->first >= begin) {
          if (it->first < FLAGS_gorilla_blacklisted_time_min ||
              it->first > FLAGS_gorilla_blacklisted_time_max) {
            LOG(INFO) << "The timestamp is " << it->first << std::endl;
            addValueToOutput(out, it->first, it->second);
            count++;
          }
        }
        ++it;
      }
      it = ++curIt;
    }

    addValueToOutput(out, firstTimestamp, firstValue);
    count++;
  }

  for (int i = 1; i < n; i++) {
    int64_t unixTime = readNextTimestamp(
        data.data(), bitPos, previousTimestamp, previousTimestampDelta);
    double value = readNextValue(
        data.data(),
        bitPos,
        previousValue,
        previousLeadingZeros,
        previousTrailingZeros);

    // Read the tombstone bit.
    tombstone = BitUtil::readValueFromBitString(data.data(), bitPos, 1);
    // Read the lock bit which is useless for the query.
    BitUtil::readValueFromBitString(data.data(), bitPos, 1);

    // Find the data point from the updatedDataMap.
    // Update the value if the data point exists.
    if (updatedDataMap && tombstone) {
      auto curIt = updatedDataMap->find(unixTime);
      // Update the value if the data point exists in the updatedDataMap.
      if (curIt != updatedDataMap->end()) {
        LOG(INFO) << "Timestamp is " << unixTime << std::endl;
        LOG(INFO) << "The original value is " << value << std::endl;
        LOG(INFO) << "The updated value is " << curIt->second << std::endl;
        value = curIt->second;
      }
      // The data point has been deleted.
      // Nothing to do. 
      else{
        it = curIt;
        continue;
      }

      // Output data points which exist just in the updatedDataMap.
      // The code works when data points in block and updatedDataMap
      // are both in order.
      while(it != curIt && it != updatedDataMap->end()) {
        if (it->first >= begin) {
          if (it->first < FLAGS_gorilla_blacklisted_time_min ||
              it->first > FLAGS_gorilla_blacklisted_time_max) {

            LOG(INFO) << "The timestamp is " << it->first << std::endl;
            addValueToOutput(out, it->first, it->second);
            count++;
          }
        }
        ++it;
      }
      it = ++curIt;
    }

    if (unixTime > end) {
      break;
    }

    if (unixTime >= begin) {
      if (unixTime < FLAGS_gorilla_blacklisted_time_min ||
          unixTime > FLAGS_gorilla_blacklisted_time_max) {
        addValueToOutput(out, unixTime, value);
        count++;
      }
    }
  }

  return count;
}

}
} // facebook::gorilla

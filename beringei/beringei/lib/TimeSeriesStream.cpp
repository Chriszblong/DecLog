/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */


#include "TimeSeriesStream.h"

#include <vector>
#include <iostream>

#include "BitUtil.h"

DEFINE_int64(
    gorilla_blacklisted_time_min,
    0,
    "Lower time for blacklisted unix times to not return when decompressing"
    " gorilla block");
DEFINE_int64(
    gorilla_blacklisted_time_max,
    0,
    "Upper time for blacklisted unix times to not return when decompressing"
    " gorilla block");

DEFINE_int64(
    data_point_nonexistent,
    -1,
    "The data point is nonexistent in the stream and the corresponding update map.");

// DEFINE_int64(
//     tombstone_setted,
//     -2,
//     "The tombstone of the data point is setted in the stream."
//     "The caller should find the data point in the update map of the stream");

DEFINE_int64(
    data_point_in_updatedDataMap,
    -3,
    "The data point is in the updatedDataMap of the corresponding stream.");

DECLARE_int32(extention_len);
DECLARE_int32(mintimestampdelta);

namespace facebook {
namespace gorilla {

struct {
  int64_t bitsForValue;
  uint32_t controlValue;
  uint32_t controlValueBitLength;
} static const timestampEncodings[4] = {
    {7, 2, 2},
    {9, 6, 3},
    {12, 14, 4},
    {32, 15, 4}};

TimeSeriesStream::TimeSeriesStream() {
  prevTimestamp_ = 0;
  reset();
}

void TimeSeriesStream::reset() {
  // Shrink to fit before clearing to possibly free some unused
  // memory. (It will only free memory if there's more than 50% extra
  // allocated.)
  data_.shrink_to_fit();

  // This won't actually release the memory.
  data_.clear();
  numBits_ = 0;
  prevTimestampDelta_ = 0;
  previousValue_ = 0;
  previousValueLeadingZeros_ = 0;
  previousValueTrailingZeros_ = 0;
  count_ = 0;
  // Do not reset the `prevTimestamp_` because it is still useful for
  // the callers and never actually used when data_ is empty.
}

uint32_t TimeSeriesStream::size() {
  return data_.size();
}

uint32_t TimeSeriesStream::capacity() {
  return data_.capacity();
}

void TimeSeriesStream::finalize(uint32_t timeseriesId, FILE* f) {
  fwrite(&timeseriesId, sizeof(timeseriesId), 1, f);
  fwrite(&previousValue_, sizeof(previousValue_), 1, f);
  fwrite(&numBits_, sizeof(numBits_), 1, f);
  fwrite(&prevTimestamp_, sizeof(prevTimestamp_), 1, f);
  fwrite(&prevTimestampDelta_, sizeof(prevTimestampDelta_), 1, f);
  fwrite(&previousValueLeadingZeros_, sizeof(previousValueLeadingZeros_), 1, f);
  fwrite(&previousValueTrailingZeros_, sizeof(previousValueTrailingZeros_), 1, f);
  fwrite(&count_, sizeof(count_), 1, f);
  size_t dataSize = data_.size();
  fwrite(&dataSize, sizeof(dataSize), 1, f);
  fwrite(data_.data(), dataSize, 1, f);
}

size_t TimeSeriesStream::deserializeStream(const char* ptr) {
  size_t readLen = 0;

  memcpy(&previousValue_, ptr, sizeof(previousValue_));
  ptr += sizeof(previousValue_);
  readLen += sizeof(previousValue_);

  memcpy(&numBits_, ptr, sizeof(numBits_));
  ptr += sizeof(numBits_);
  readLen += sizeof(numBits_);

  memcpy(&prevTimestamp_, ptr, sizeof(prevTimestamp_));
  ptr += sizeof(prevTimestamp_);
  readLen += sizeof(prevTimestamp_);

  memcpy(&prevTimestampDelta_, ptr, sizeof(prevTimestampDelta_));
  ptr += sizeof(prevTimestampDelta_);
  readLen += sizeof(prevTimestampDelta_);

  memcpy(&previousValueLeadingZeros_, ptr, sizeof(previousValueLeadingZeros_));
  ptr += sizeof(previousValueLeadingZeros_);
  readLen += sizeof(previousValueLeadingZeros_);

  memcpy(&previousValueTrailingZeros_, ptr, sizeof(previousValueTrailingZeros_));
  ptr += sizeof(previousValueTrailingZeros_);
  readLen += sizeof(previousValueTrailingZeros_);

  memcpy(&count_, ptr, sizeof(count_));
  ptr += sizeof(count_);
  readLen += sizeof(count_);

  size_t dataSize;
  memcpy(&dataSize, ptr, sizeof(dataSize));
  ptr += sizeof(dataSize);
  readLen += sizeof(dataSize);
  
  data_.append(ptr, dataSize);
  ptr += dataSize;
  readLen += dataSize;

  return readLen;
}

void TimeSeriesStream::readData(char* out, uint32_t size) {
  memcpy(out, data_.data(), size);
}

void TimeSeriesStream::readData(std::string& out) {
  out = data_.toStdString();
}

const char* TimeSeriesStream::getDataPtr() {
  return data_.data();
}

bool TimeSeriesStream::append(
    const TimeValuePair& value,
    int64_t minTimestampDelta) {
  // if (numBits_ == 0) {
  //   firstValue_ = value.value;
  // }
  return append(value.unixTime, value.value, minTimestampDelta);
}

bool TimeSeriesStream::append(
    int64_t unixTime,
    double value,
    int64_t minTimestampDelta) {
  if (!appendTimestamp(unixTime, minTimestampDelta)) {
    return false;
  }

  appendValue(value);

  // Add one bit as a tombstone and one bit as a lock.
  // For the inserting data point, the two bits are zero.
  appendLockAndTombstone();
  ++count_;
  return true;
}

bool TimeSeriesStream::appendTimestamp(
    int64_t timestamp,
    int64_t minTimestampDelta) {
  if (data_.empty()) {
    // Store the first value as is
    BitUtil::addValueToBitString(
        timestamp, kBitsForFirstTimestamp, data_, numBits_);
    prevTimestamp_ = timestamp;
    prevTimestampDelta_ = kDefaultDelta;
    return true;
  }

  // Store a delta of delta for the rest of the values in one of the
  // following ways
  //
  // '0' = delta of delta did not change
  // '10' followed by a value length of 7
  // '110' followed by a value length of 9
  // '1110' followed by a value length of 12
  // '1111' followed by a value length of 32

  int64_t delta = timestamp - prevTimestamp_;
  if (delta < minTimestampDelta) {
    return false;
  }

  int64_t deltaOfDelta = delta - prevTimestampDelta_;

  if (deltaOfDelta == 0) {
    prevTimestamp_ = timestamp;
    BitUtil::addValueToBitString(0, 1, data_, numBits_);
    return true;
  }

  if (deltaOfDelta > 0) {
    // There are no zeros. Shift by one to fit in x number of bits
    deltaOfDelta--;
  }

  int64_t absValue = std::abs(deltaOfDelta);

  for (int i = 0; i < 4; i++) {
    if (absValue < ((int64_t)1 << (timestampEncodings[i].bitsForValue - 1))) {
      BitUtil::addValueToBitString(
          timestampEncodings[i].controlValue,
          timestampEncodings[i].controlValueBitLength,
          data_,
          numBits_);

      // Make this value between [0, 2^timestampEncodings[i].bitsForValue - 1]
      int64_t encodedValue = deltaOfDelta +
          ((int64_t)1 << (timestampEncodings[i].bitsForValue - 1));

      BitUtil::addValueToBitString(
          encodedValue, timestampEncodings[i].bitsForValue, data_, numBits_);
      break;
    }
  }

  prevTimestamp_ = timestamp;
  prevTimestampDelta_ = delta;

  return true;
}

void TimeSeriesStream::appendValue(double value) {
  uint64_t* p = (uint64_t*)&value;
  uint64_t xorWithPrevius = previousValue_ ^ *p;

  // Doubles are encoded by XORing them with the previous value.  If
  // XORing results in a zero value (value is the same as the previous
  // value), only a single zero bit is stored, otherwise 1 bit is
  // stored. TODO : improve this with RLE for the number of zeros
  //
  // For non-zero XORred results, there are two choices:
  //
  // 1) If the block of meaningful bits falls in between the block of
  //    previous meaningful bits, i.e., there are at least as many
  //    leading zeros and as many trailing zeros as with the previous
  //    value, use that information for the block position and just
  //    store the XORred value.
  //
  // 2) Length of the number of leading zeros is stored in the next 5
  //    bits, then length of the XORred value is stored in the next 6
  //    bits and finally the XORred value is stored.

  if (xorWithPrevius == 0) {
    BitUtil::addValueToBitString(0, 1, data_, numBits_);
    return;
  }

  BitUtil::addValueToBitString(1, 1, data_, numBits_);

  int leadingZeros = __builtin_clzll(xorWithPrevius);
  int trailingZeros = __builtin_ctzll(xorWithPrevius);

  if (leadingZeros > kMaxLeadingZerosLength) {
    leadingZeros = kMaxLeadingZerosLength;
  }

  int blockSize = 64 - leadingZeros - trailingZeros;
  uint32_t expectedSize =
      kLeadingZerosLengthBits + kBlockSizeLengthBits + blockSize;
  uint32_t previousBlockInformationSize =
      64 - previousValueTrailingZeros_ - previousValueLeadingZeros_;

  if (leadingZeros >= previousValueLeadingZeros_ &&
      trailingZeros >= previousValueTrailingZeros_ &&
      previousBlockInformationSize < expectedSize) {
    // Control bit for using previous block information.
    BitUtil::addValueToBitString(1, 1, data_, numBits_);

    uint64_t blockValue = xorWithPrevius >> previousValueTrailingZeros_;
    BitUtil::addValueToBitString(
        blockValue, previousBlockInformationSize, data_, numBits_);

  } else {
    // Control bit for not using previous block information.
    BitUtil::addValueToBitString(0, 1, data_, numBits_);

    BitUtil::addValueToBitString(
        leadingZeros, kLeadingZerosLengthBits, data_, numBits_);

    BitUtil::addValueToBitString(
        // To fit in 6 bits. There will never be a zero size block
        blockSize - kBlockSizeAdjustment,
        kBlockSizeLengthBits,
        data_,
        numBits_);

    uint64_t blockValue = xorWithPrevius >> trailingZeros;
    BitUtil::addValueToBitString(blockValue, blockSize, data_, numBits_);

    previousValueTrailingZeros_ = trailingZeros;
    previousValueLeadingZeros_ = leadingZeros;
  }

  previousValue_ = *p;
}

void TimeSeriesStream::appendLockAndTombstone() {
  BitUtil::addValueToBitString(0, 2, data_, numBits_);
}

int64_t TimeSeriesStream::readNextTimestamp(
    const char* data,
    uint64_t& bitPos,
    int64_t& prevValue,
    int64_t& prevDelta) {
  uint32_t type = BitUtil::findTheFirstZeroBit(data, bitPos, 4);
  if (type > 0) {
    // Delta of delta is non zero. Calculate the new delta. `index`
    // will be used to find the right length for the value that is
    // read.
    int index = type - 1;
    int64_t decodedValue = BitUtil::readValueFromBitString(
        data, bitPos, timestampEncodings[index].bitsForValue);

    // [0,255] becomes [-128,127]
    decodedValue -=
        ((int64_t)1 << (timestampEncodings[index].bitsForValue - 1));
    if (decodedValue >= 0) {
      // [-128,127] becomes [-128,128] without the zero in the middle
      decodedValue++;
    }

    prevDelta += decodedValue;
  }

  prevValue += prevDelta;
  return prevValue;
}

double TimeSeriesStream::readNextValue(
    const char* data,
    uint64_t& bitPos,
    uint64_t& previousValue,
    uint64_t& previousLeadingZeros,
    uint64_t& previousTrailingZeros) {
  uint64_t nonZeroValue = BitUtil::readValueFromBitString(data, bitPos, 1);

  if (!nonZeroValue) {
    double* p = (double*)&previousValue;
    return *p;
  }

  uint64_t usePreviousBlockInformation =
      BitUtil::readValueFromBitString(data, bitPos, 1);

  uint64_t xorValue;
  if (usePreviousBlockInformation) {
    xorValue = BitUtil::readValueFromBitString(
        data, bitPos, 64 - previousLeadingZeros - previousTrailingZeros);
    xorValue <<= previousTrailingZeros;
  } else {
    uint64_t leadingZeros =
        BitUtil::readValueFromBitString(data, bitPos, kLeadingZerosLengthBits);
    uint64_t blockSize =
        BitUtil::readValueFromBitString(data, bitPos, kBlockSizeLengthBits) +
        kBlockSizeAdjustment;

    previousTrailingZeros = 64 - blockSize - leadingZeros;
    xorValue = BitUtil::readValueFromBitString(data, bitPos, blockSize);
    xorValue <<= previousTrailingZeros;
    previousLeadingZeros = leadingZeros;
  }

  uint64_t value = xorValue ^ previousValue;
  previousValue = value;

  double* p = (double*)&value;
  return *p;
}

uint32_t TimeSeriesStream::getFirstTimeStamp() {
  if (data_.length() == 0) {
    return 0;
  }

  uint64_t bitPos = 0;
  return BitUtil::readValueFromBitString(
      data_.c_str(), bitPos, kBitsForFirstTimestamp);
}

// int TimeSeriesStream::findDataPointInStream(
//     TimeSeriesStream& block,
//     const TimeValuePair& value,
//     const uint16_t& itemCount) {

//   uint64_t previousValue = 0;
//   uint64_t previousLeadingZeros = 0;
//   uint64_t previousTrailingZeros = 0;
//   uint64_t bitPos = 0;
//   int64_t previousTimestampDelta = kDefaultDelta;
//   int64_t firstTimestamp =
//       BitUtil::readValueFromBitString(block.getDataPtr(), bitPos,
//       kBitsForFirstTimestamp);
//   double firstValue = readNextValue(
//       block.getDataPtr(),
//       bitPos,
//       previousValue,
//       previousLeadingZeros,
//       previousTrailingZeros);

//   // The minimum timestamp is large than value.unixTime.
//   // So the queried data point must not exist.
//   if (firstTimestamp > value.unixTime) {
//     return -1;
//   }

//   if (firstTimestamp == value.unixTime) {
//     return bitPos;
//   }

//   // Read two bits of lock and tombstone.
//   BitUtil::readValueFromBitString(block.getDataPtr(), bitPos, 2);

//   int64_t previousTimestamp = firstTimestamp;

//   // Read the remaining values.
//   for (int i = 1; i < itemCount; i++) {
//     // Read timestamp.
//     int64_t unixTime = readNextTimestamp(
//         block.getDataPtr(), bitPos, previousTimestamp,
//         previousTimestampDelta);

//     // Read value.
//     readNextValue(
//         block.getDataPtr(),
//         bitPos,
//         previousValue,
//         previousLeadingZeros,
//         previousTrailingZeros);

//     // The timestamp is large than value.unixTime.
//     // So the queried data point must not exist.
//     if (unixTime > value.unixTime) {
//       return -1;
//     }

//     if (unixTime == value.unixTime) {
//       return bitPos;
//     }

//     // Read two bits of lock and tombstone.
//     BitUtil::readValueFromBitString(block.getDataPtr(), bitPos, 2);
//   }
//   return bitPos;
// }

// written by yls
// 给stroage block内的某范围内数据点原地上锁
bool TimeSeriesStream::lockInExtension(
    char* block,
    uint16_t dataLength,
    uint16_t itemCount,
    int64_t begin,
    int64_t end) {
  if (itemCount == 0) {
    return true;
  }
  std::vector<uint64_t> lockPos;
  // reserve(&out, out.size() + n);
  uint64_t previousValue = 0;
  uint64_t previousLeadingZeros = 0;
  uint64_t previousTrailingZeros = 0;
  uint64_t bitPos = 0;
  int64_t previousTimestampDelta = kDefaultDelta;
  int64_t firstTimestamp =
      BitUtil::readValueFromBitString(block, bitPos, kBitsForFirstTimestamp);
  double firstValue = readNextValue(
      block,
      bitPos,
      previousValue,
      previousLeadingZeros,
      previousTrailingZeros);
  // 读extension拓展位中的tombstone
  // BitUtil::readValueFromBitString(block, bitPos, 1);
  bitPos++;
  // 读extension拓展位中的锁位
  uint64_t lock = BitUtil::readValueFromBitString(block, bitPos, 1);
  int64_t previousTimestamp = firstTimestamp;
  // 超过end范围
  if (firstTimestamp > end) {
    return true;
  }
  int count = 0;
  if (firstTimestamp >= begin) {
    // 检查锁位
    if (lock == 1)
      return false;
    lockPos.emplace_back(bitPos - 1);
    count++;
  }
  // 读取剩下的数据
  for (int i = 1; i < itemCount; i++) {
    // 读取timestamp
    int64_t unixTime = readNextTimestamp(
        block, bitPos, previousTimestamp, previousTimestampDelta);
    // 读取value
    double value = readNextValue(
        block,
        bitPos,
        previousValue,
        previousLeadingZeros,
        previousTrailingZeros);
    // 读extension拓展位中的tombstone
    // BitUtil::readValueFromBitString(block, bitPos, 1);
    bitPos++;
    // 读extension拓展位中的锁位
    lock = BitUtil::readValueFromBitString(block, bitPos, 1);
    if (unixTime > end) {
      break;
    }
    if (unixTime >=
        begin) { // FLAGS_gorilla_blacklisted_time_min==FLAGS_gorilla_blacklisted_time_max=0
      if (unixTime < FLAGS_gorilla_blacklisted_time_min ||
          unixTime > FLAGS_gorilla_blacklisted_time_max) {
        // 检查锁位
        if (lock == 1)
          return false;
        lockPos.emplace_back(bitPos - 1);
        count++;
      }
    }
  }
  uint64_t bytePos;
  uint64_t bitRemain;
  for (int i = 0; i < lockPos.size(); i++) {
    // 上锁
    bytePos = lockPos[i] / 8;
    bitRemain = lockPos[i] % 8;
    block[bytePos] = block[bytePos] | (128 >> bitRemain);
  }

  return true;
}

// written by yls
// 给stream block内的某范围内数据点原地上锁
bool TimeSeriesStream::lockInExtension(
    TimeSeriesStream& block,
    uint16_t dataLength,
    uint16_t itemCount,
    int64_t begin,
    int64_t end) {
  if (itemCount == 0) {
    return true;
  }
  std::vector<uint64_t> lockPos;
  // reserve(&out, out.size() + n);
  uint64_t previousValue = 0;
  uint64_t previousLeadingZeros = 0;
  uint64_t previousTrailingZeros = 0;
  uint64_t bitPos = 0;
  int64_t previousTimestampDelta = kDefaultDelta;
  int64_t firstTimestamp = BitUtil::readValueFromBitString(
      block.getDataPtr(), bitPos, kBitsForFirstTimestamp);
  double firstValue = readNextValue(
      block.getDataPtr(),
      bitPos,
      previousValue,
      previousLeadingZeros,
      previousTrailingZeros);
  // 读extension拓展位中的tombstone
  // BitUtil::readValueFromBitString(block.getDataPtr(), bitPos, 1);
  bitPos++;
  // 读extension拓展位中的锁位
  uint64_t lock =
      BitUtil::readValueFromBitString(block.getDataPtr(), bitPos, 1);
  int64_t previousTimestamp = firstTimestamp;
  // 超过end范围
  if (firstTimestamp > end) {
    return true;
  }
  int count = 0;
  if (firstTimestamp >= begin) {
    // 检查锁位
    if (lock == 1)
      return false;
    lockPos.emplace_back(bitPos - 1);
    count++;
  }
  // 读取剩下的数据
  for (int i = 1; i < itemCount; i++) {
    // 读取timestamp
    int64_t unixTime = readNextTimestamp(
        block.getDataPtr(), bitPos, previousTimestamp, previousTimestampDelta);
    // 读取value
    double value = readNextValue(
        block.getDataPtr(),
        bitPos,
        previousValue,
        previousLeadingZeros,
        previousTrailingZeros);
    // 读extension拓展位中的tombstone
    // BitUtil::readValueFromBitString(block.getDataPtr(), bitPos, 1);
    bitPos++;
    // 读extension拓展位中的锁位
    lock = BitUtil::readValueFromBitString(block.getDataPtr(), bitPos, 1);
    if (unixTime > end) {
      break;
    }
    if (unixTime >=
        begin) { // FLAGS_gorilla_blacklisted_time_min==FLAGS_gorilla_blacklisted_time_max=0
      if (unixTime < FLAGS_gorilla_blacklisted_time_min ||
          unixTime > FLAGS_gorilla_blacklisted_time_max) {
        // 检查锁位
        if (lock == 1)
          return false;
        lockPos.emplace_back(bitPos - 1);
        count++;
      }
    }
  }
  uint64_t bytePos;
  uint64_t bitRemain;
  for (int i = 0; i < lockPos.size(); i++) {
    // 上锁
    bytePos = lockPos[i] / 8;
    bitRemain = lockPos[i] % 8;
    block.data_[bytePos] = block.data_[bytePos] | (128 >> bitRemain);
  }

  return true;
}

// written by yls
// 给block内的某范围内数据点原地解锁
bool TimeSeriesStream::unlockInExtension(
    char* block,
    uint16_t dataLength,
    uint16_t itemCount,
    int64_t begin,
    int64_t end) {
  if (itemCount == 0) {
    return true;
  }
  std::vector<uint64_t> lockPos;
  // reserve(&out, out.size() + n);
  uint64_t previousValue = 0;
  uint64_t previousLeadingZeros = 0;
  uint64_t previousTrailingZeros = 0;
  uint64_t bitPos = 0;
  int64_t previousTimestampDelta = kDefaultDelta;
  int64_t firstTimestamp =
      BitUtil::readValueFromBitString(block, bitPos, kBitsForFirstTimestamp);
  double firstValue = readNextValue(
      block,
      bitPos,
      previousValue,
      previousLeadingZeros,
      previousTrailingZeros);
  // 读extension拓展位中的tombstone
  // BitUtil::readValueFromBitString(block, bitPos, 1);
  bitPos++;
  // 读extension拓展位中的锁位
  uint64_t lock = BitUtil::readValueFromBitString(block, bitPos, 1);
  int64_t previousTimestamp = firstTimestamp;
  // 超过end范围
  if (firstTimestamp > end) {
    return true;
  }
  int count = 0;
  if (firstTimestamp >= begin) {
    lockPos.emplace_back(bitPos - 1);
    count++;
  }
  // 读取剩下的数据
  for (int i = 1; i < itemCount; i++) {
    // 读取timestamp
    int64_t unixTime = readNextTimestamp(
        block, bitPos, previousTimestamp, previousTimestampDelta);
    // 读取value
    double value = readNextValue(
        block,
        bitPos,
        previousValue,
        previousLeadingZeros,
        previousTrailingZeros);
    // 读extension拓展位中的tombstone
    // BitUtil::readValueFromBitString(block, bitPos, 1);
    bitPos++;
    // 读extension拓展位中的锁位
    lock = BitUtil::readValueFromBitString(block, bitPos, 1);
    if (unixTime > end) {
      break;
    }
    if (unixTime >=
        begin) { // FLAGS_gorilla_blacklisted_time_min==FLAGS_gorilla_blacklisted_time_max=0
      if (unixTime < FLAGS_gorilla_blacklisted_time_min ||
          unixTime > FLAGS_gorilla_blacklisted_time_max) {
        lockPos.emplace_back(bitPos - 1);
        count++;
      }
    }
  }
  uint64_t bytePos;
  uint64_t bitRemain;
  for (int i = 0; i < lockPos.size(); i++) {
    // 解锁
    bytePos = lockPos[i] / 8;
    bitRemain = lockPos[i] % 8;
    block[bytePos] = block[bytePos] & ~(128 >> bitRemain);
  }

  return true;
}

// written by yls
// 给stream block内的某范围内数据点原地解锁
bool TimeSeriesStream::unlockInExtension(
    TimeSeriesStream& block,
    uint16_t dataLength,
    uint16_t itemCount,
    int64_t begin,
    int64_t end) {
  if (itemCount == 0) {
    return true;
  }
  std::vector<uint64_t> lockPos;
  // reserve(&out, out.size() + n);
  uint64_t previousValue = 0;
  uint64_t previousLeadingZeros = 0;
  uint64_t previousTrailingZeros = 0;
  uint64_t bitPos = 0;
  int64_t previousTimestampDelta = kDefaultDelta;
  int64_t firstTimestamp = BitUtil::readValueFromBitString(
      block.getDataPtr(), bitPos, kBitsForFirstTimestamp);
  double firstValue = readNextValue(
      block.getDataPtr(),
      bitPos,
      previousValue,
      previousLeadingZeros,
      previousTrailingZeros);
  // 读extension拓展位中的tombstone
  // BitUtil::readValueFromBitString(block.getDataPtr(), bitPos, 1);
  bitPos++;
  // 读extension拓展位中的锁位
  uint64_t lock =
      BitUtil::readValueFromBitString(block.getDataPtr(), bitPos, 1);
  int64_t previousTimestamp = firstTimestamp;
  // 超过end范围
  if (firstTimestamp > end) {
    return true;
  }
  int count = 0;
  if (firstTimestamp >= begin) {
    lockPos.emplace_back(bitPos - 1);
    count++;
  }
  // 读取剩下的数据
  for (int i = 1; i < itemCount; i++) {
    // 读取timestamp
    int64_t unixTime = readNextTimestamp(
        block.getDataPtr(), bitPos, previousTimestamp, previousTimestampDelta);
    // 读取value
    double value = readNextValue(
        block.getDataPtr(),
        bitPos,
        previousValue,
        previousLeadingZeros,
        previousTrailingZeros);
    // 读extension拓展位中的tombstone
    // BitUtil::readValueFromBitString(block.getDataPtr(), bitPos, 1);
    bitPos++;
    // 读extension拓展位中的锁位
    lock = BitUtil::readValueFromBitString(block.getDataPtr(), bitPos, 1);
    if (unixTime > end) {
      break;
    }
    if (unixTime >=
        begin) { // FLAGS_gorilla_blacklisted_time_min==FLAGS_gorilla_blacklisted_time_max=0
      if (unixTime < FLAGS_gorilla_blacklisted_time_min ||
          unixTime > FLAGS_gorilla_blacklisted_time_max) {
        lockPos.emplace_back(bitPos - 1);
        count++;
      }
    }
  }
  uint64_t bytePos;
  uint64_t bitRemain;
  for (int i = 0; i < lockPos.size(); i++) {
    // 解锁
    bytePos = lockPos[i] / 8;
    bitRemain = lockPos[i] % 8;
    block.data_[bytePos] = block.data_[bytePos] & ~(128 >> bitRemain);
  }

  return true;
}

void TimeSeriesStream::flushUpdate(
    const char* data,
    uint16_t dataLength,
    uint16_t itemCount,
    std::map<int64_t, folly::fbstring>& updatedDataMap,
    TimeSeriesStream& block) {
  if (itemCount == 0) {
    return;
  }

  std::size_t extention_len = FLAGS_extention_len >> 3 + 1;
  uint64_t previousValue = 0;
  uint64_t previousLeadingZeros = 0;
  uint64_t previousTrailingZeros = 0;
  uint64_t bitPos = 0;
  int64_t previousTimestampDelta = kDefaultDelta;
  int64_t firstTimestamp =
      BitUtil::readValueFromBitString(data, bitPos, kBitsForFirstTimestamp);
  double firstValue = readNextValue(
      data, bitPos, previousValue, previousLeadingZeros, previousTrailingZeros);
  // 读extension拓展位中的tombstone
  uint64_t tombstone = BitUtil::readValueFromBitString(data, bitPos, 1);
  bitPos++;
  // 读extension拓展位中的锁位
  // uint64_t lock = BitUtil::readValueFromBitString(block, bitPos, 1);
  if (tombstone == 1) {
    auto it = updatedDataMap.find(firstTimestamp);
    if (it != updatedDataMap.end() && it->second.size() > extention_len) {
      uint64_t tmp = 0;
      tmp = BitUtil::readValueFromBitString(it->second.c_str(), tmp, 64);
      firstValue = *(double*)&tmp;
      block.append(firstTimestamp, firstValue, FLAGS_mintimestampdelta);
    }
  } else {
    block.append(firstTimestamp, firstValue, FLAGS_mintimestampdelta);
  }

  int64_t previousTimestamp = firstTimestamp;

  // 读取剩下的数据
  for (int i = 1; i < itemCount; i++) {
    // 读取timestamp
    int64_t unixTime = readNextTimestamp(
        data, bitPos, previousTimestamp, previousTimestampDelta);
    // 读取value
    double value = readNextValue(
        data,
        bitPos,
        previousValue,
        previousLeadingZeros,
        previousTrailingZeros);
    // 读extension拓展位中的tombstone
    tombstone = BitUtil::readValueFromBitString(data, bitPos, 1);
    bitPos++;
    // 读extension拓展位中的锁位
    // lock = BitUtil::readValueFromBitString(block, bitPos, 1);
    if (tombstone == 1) {
      auto it = updatedDataMap.find(unixTime);
      if (it != updatedDataMap.end() && it->second.size() > extention_len) {
        uint64_t tmp = 0;
        tmp = BitUtil::readValueFromBitString(it->second.c_str(), tmp, 64);
        value = *(double*)&tmp;
        block.append(unixTime, value, FLAGS_mintimestampdelta);
      }
    } else {
      block.append(unixTime, value, FLAGS_mintimestampdelta);
    }
  }
}

void TimeSeriesStream::createUpdatedDataMap(
    folly::StringPiece data,
    int itemCount,
    std::map<int64_t, double>* updatedDataMap) {
  if (data.empty() || itemCount == 0) {
    return;
  }

  uint64_t bitPos = 0;

  for (int i = 0; i < itemCount; ++i) {
    uint64_t unixTime = BitUtil::readValueFromBitString(data.data(), bitPos, 64);
    LOG(INFO) << "Current Unixtime is " << unixTime << std::endl;
    uint64_t value = BitUtil::readValueFromBitString(data.data(), bitPos, 64);
    double lastValue = *(double*)(&value);
    updatedDataMap->insert(std::make_pair((int64_t)(unixTime), lastValue));
  }
}

int64_t TimeSeriesStream::findDataPointInStream(
    const char* block,
    const TimeValuePair& value,
    const uint16_t& itemCount,
    int64_t& targetTimestamp) {
  uint64_t previousValue = 0;
  uint64_t previousLeadingZeros = 0;
  uint64_t previousTrailingZeros = 0;
  uint64_t bitPos = 0;
  int64_t previousTimestampDelta = kDefaultDelta;
  int64_t firstTimestamp =
      BitUtil::readValueFromBitString(block, bitPos, kBitsForFirstTimestamp);
  double firstValue = readNextValue(
      block,
      bitPos,
      previousValue,
      previousLeadingZeros,
      previousTrailingZeros);

  // The minimum timestamp is large than value.unixTime.
  // So the queried data point must not exist.
  if (firstTimestamp > value.unixTime) {
    LOG(INFO) << "First timestamp " << firstTimestamp
              << " is larger than the target timestamp" << value.unixTime
              << std::endl;
    return FLAGS_data_point_nonexistent;
  }

  if (firstTimestamp == value.unixTime) {
    return bitPos;
  }

  // Read two bits of lock and tombstone.
  BitUtil::readValueFromBitString(block, bitPos, 2);

  int64_t previousTimestamp = firstTimestamp;
  int64_t unixTime;
  // Read the remaining values.
  for (int i = 1; i < itemCount; i++) {
    // Read timestamp.
    unixTime = readNextTimestamp(
        block, bitPos, previousTimestamp, previousTimestampDelta);

    // LOG(INFO) << "Current timestamp in the stream is " << unixTime <<
    // std::endl;

    // Read value.
    readNextValue(
        block,
        bitPos,
        previousValue,
        previousLeadingZeros,
        previousTrailingZeros);

    // The timestamp is large than value.unixTime.
    // So the queried data point must not exist.
    if (unixTime > value.unixTime) {
      LOG(INFO) << "Current timestamp " << unixTime
                << " is larger than the target timestamp" << value.unixTime
                << std::endl;
      return FLAGS_data_point_nonexistent;
    }

    if (unixTime == value.unixTime) {
      // Read the tombstone
      uint64_t tombstone = BitUtil::readValueFromBitString(block, bitPos, 1);
      // // The data point has been deleted.
      // if (tombstone) {
      //   return FLAGS_tombstone_setted;
      // }

      // LOG(INFO) << "The target block address is  " << block << std::endl;

      // LOG(INFO) << "The original tombstoneBit is " << (bitPos - 1) <<
      // std::endl;

      // LOG(INFO) << "The original tombstone bit is " << tombstone <<
      // std::endl;

      // LOG(INFO) << "The original lock bit is " <<
      // BitUtil::readValueFromBitString(block, bitPos, 1) << std::endl;
      // --bitPos;

      targetTimestamp = unixTime;
      return (bitPos - 1);
    }

    // Read two bits of lock and tombstone.
    BitUtil::readValueFromBitString(block, bitPos, 2);
  }

  LOG(INFO) << "There is no timestamp in the strem, the last timestamp is "
            << unixTime << "The target timestamp is " << value.unixTime
            << std::endl;
  return FLAGS_data_point_nonexistent;
}

} // namespace gorilla
} // namespace facebook

/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "beringei/client/RequestBatchingQueue.h"

namespace facebook {
namespace gorilla {

bool RequestBatchingQueue::push(std::vector<DataPoint>& points) {
  int numPoints = points.size();

  // Ignore empty vectors, as these are shutdown markers.
  if (numPoints == 0) {
    return true;
  }

  // capacity_ isn't actually a hard capacity. It's okay if multiple
  // threads are running here.
  if (numQueuedDataPoints_ + numPoints > static_cast<int>(capacity_)) {
    LOG(ERROR) << "Queue does not have any more capacity! "
               << numQueuedDataPoints_ << " + " << numPoints << " > "
               << capacity_ << ", Actual queue size : " << queue_.size();
    return false;
  }

  numQueuedDataPoints_ += numPoints;
  if (!queue_.write(std::move(points))) {
    numQueuedDataPoints_ -= numPoints;
    LOG(ERROR) << "Queue is full!";
    return false;
  }

  return true;
}

std::pair<bool, int> RequestBatchingQueue::pop(
    std::function<bool(DataPoint& dp)> popCallback) {
  std::vector<DataPoint> points;
  queue_.blockingRead(points);
  int popped = 0;
  bool continuePopping = true;
  do {
    if (points.size() == 0) {
      // Signals shutdown.
      return {false, popped};
    }
    numQueuedDataPoints_ -= points.size();

    for (auto& dp : points) {
      if (!popCallback(dp)) {
        // Callback has had enough, but still push all the points in
        // this vector.
        continuePopping = false;
      }
      popped++;
    }

  } while (continuePopping && queue_.read(points));

  return {true, popped};
}

void RequestBatchingQueue::flush(int n) {
  // Add one empty vector per thread.
  for (int i = 0; i < n; i++) {
    std::vector<DataPoint> points;
    queue_.blockingWrite(std::move(points));
  }
}
}
} // facebook::gorilla

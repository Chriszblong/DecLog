# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

# Include the PMDK libraries.
include_directories(/usr/local/include) 
link_directories(/usr/local/lib)
link_directories(/usr/lib/x86_64-linux-gnu/)
link_libraries(
    pmem
    ipmctl
    pmem2
    ndctl
)

add_library(
    beringei_client STATIC

    BeringeiClient.h
    BeringeiClientImpl.h
    BeringeiConfigurationAdapterIf.h
    BeringeiNetworkClient.h
    RequestBatchingQueue.h
    BeringeiClient.cpp
    BeringeiClientImpl.cpp
    BeringeiNetworkClient.cpp
    RequestBatchingQueue.cpp
)
target_link_libraries(
    beringei_client

    beringei_core
    beringei_thrift
    ${FOLLY_LIBRARIES}
)

add_subdirectory(tests)

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

add_executable(
    beringei_client_test_bin

    MockConfigurationAdapter.h
    BeringeiClientTest.cpp
    RequestBatchingQueueTest.cpp
)
target_link_libraries(
    beringei_client_test_bin

    beringei_core
    beringei_client
    ${FOLLY_LIBRARIES}
    ${GTEST_HARNESS_LIBS}
    ${LIBGLOG_LIBRARY}
    ${GFLAGS_LIBRARIES}
)

add_test(beringei_client_tests beringei_client_test_bin)

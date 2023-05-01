/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Basic DB that just prints out the requested operations, instead of doing them against a database.
 */
public class GoodBadUglyDB extends DB {
    public static final String SIMULATE_DELAY = "gbudb.delays";
    public static final String SIMULATE_DELAY_DEFAULT = "200,1000,10000,50000,100000";
    static ReadWriteLock DB_ACCESS = new ReentrantReadWriteLock();
    long delays[];

    public GoodBadUglyDB() {
        delays = new long[]{200, 1000, 10000, 50000, 200000};
    }

    void delay() {
        final Random random = Utils.random();
        double p = random.nextDouble();
        int mod;
        if (p < 0.9) {
            mod = 0;
        } else if (p < 0.99) {
            mod = 1;
        } else if (p < 0.9999) {
            mod = 2;
        } else {
            mod = 3;
        }
        // this will make mod 3 pauses global
        Lock lock = mod == 3 ? DB_ACCESS.writeLock() : DB_ACCESS.readLock();
        if (mod == 3) {
            System.out.println("OUCH");
        }
        lock.lock();
        try {
            final long baseDelayNs = MICROSECONDS.toNanos(delays[mod]);
            final int delayRangeNs = (int) (MICROSECONDS.toNanos(delays[mod + 1]) - baseDelayNs);
            final long delayNs = baseDelayNs + random.nextInt(delayRangeNs);
            long now = System.nanoTime();
            final long deadline = now + delayNs;
            do {
                LockSupport.parkNanos(deadline - now);
            } while ((now = System.nanoTime()) < deadline && !Thread.interrupted());
        } finally {
            lock.unlock();
        }

    }

    /**
     * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() {
        int i = 0;
        for (String delay : getProperties().getProperty(SIMULATE_DELAY, SIMULATE_DELAY_DEFAULT).split(",")) {
            delays[i++] = Long.parseLong(delay);
        }
    }

    /**
     * Read a record from the database. Each value from the result will be stored in a HashMap (should only be one).
     *
     * @param metric The name of the metric
     * @param timestamp The timestamp of the record to read.
     * @param tags     actual tags that were want to receive (can be empty)
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public int read(String metric, Timestamp timestamp, HashMap<String, ArrayList<String>> tags) {
        delay();
        return 0;
    }

    /**
     * Perform a range scan for a set of records in the database. Each value from the result will be stored in a HashMap.
     *
     * @param metric The name of the metric
     * @param startTs The timestamp of the first record to read.
     * @param endTs The timestamp of the last record to read.
     * @param tags  actual tags that were want to receive (can be empty)
     * @param avg    do averageing
     * @param sum    do summarize
     * @param count  do count
     * @param timeValue  value for timeUnit for sum/count/avg
     * @param timeUnit  timeUnit for sum/count/avg
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public int scan(String metric, Timestamp startTs, Timestamp endTs, HashMap<String,
            ArrayList<String>> tags, boolean avg, boolean count, boolean sum, int timeValue,  TimeUnit timeUnit ) {
        delay();

        return 0;
    }

    /**
     * Insert a record in the database. Any tags/tagvalue pairs in the specified tags HashMap and the given value
     * will be written into the record with the specified timestamp
     *
     * @param metric The name of the metric
     * @param timestamp The timestamp of the record to insert.
     * @param value actual value to insert
     * @param tags A HashMap of tag/tagvalue pairs to insert as tags
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public int insert(String metric, Timestamp timestamp, double value, HashMap<String, ByteIterator> tags) {
        delay();
        return 0;
    }

}

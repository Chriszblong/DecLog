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

import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.workloads.MyWorkload;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 */
public class DBWrapper extends DB {
    DB _db;
    Measurements _measurements;

    public DBWrapper(DB db) {
        _db = db;
        _measurements = Measurements.getMeasurements();
    }

    /**
     * Get the set of properties for this DB.
     */
    public Properties getProperties() {
        return _db.getProperties();
    }

    /**
     * Set the properties for this DB.
     */
    public void setProperties(Properties p) {
        _db.setProperties(p);
    }

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws DBException {
        _db.init();
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        _db.cleanup();
        long en = System.nanoTime();
        measure("CLEANUP", ist, st, en);
    }

    /**
     * Read a record from the database. Each value from the result will be stored in a HashMap (should only be one).
     *
     * @param metric The name of the metric
     * @param timestamp The timestamp of the record to read.
     * @param tags  actual tags that were want to receive (can be empty)
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public int read(String metric, Timestamp timestamp, HashMap<String, ArrayList<String>> tags) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        int res = _db.read(metric, timestamp, tags);
        long en = System.nanoTime();
        measure("READ", ist, st, en);
        _measurements.reportReturnCode("READ", res);
        return res;
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
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        int res = _db.scan(metric, startTs, endTs, tags, avg, count, sum, timeValue, timeUnit);
        long en = System.nanoTime();
        String operation = "SCAN";
        if (avg) {
            operation = "AVG";
        }
        else if (count) {
            operation = "COUNT";
        }
        else if (sum) {
            operation = "SUM";
        }
        measure(operation, ist, st, en);
        _measurements.reportReturnCode(operation, res);
        return res;
    }

    private void measure(String op, long intendedStartTimeNanos, long startTimeNanos, long endTimeNanos) {
        _measurements.measure(op, (int) ((endTimeNanos - startTimeNanos) / 1000));
        _measurements.measureIntended(op, (int) ((endTimeNanos - intendedStartTimeNanos) / 1000));
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
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        int res = _db.insert(metric, timestamp, value, tags);
        long en = System.nanoTime();
        measure("INSERT", ist, st, en);
        _measurements.reportReturnCode("INSERT", res);
        return res;
    }

    @Override
    public int update(String metric, Timestamp timestamp, double value, HashMap<String, ByteIterator> tags) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        int res = _db.update(metric, timestamp, value, tags);
        long en = System.nanoTime();
        measure("UPDATE", ist, st, en);
        _measurements.reportReturnCode("UPDATE", res);
        return res;
    }

    @Override
    public int delete(String metric, Timestamp timestamp, HashMap<String, ByteIterator> tags) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        int res = _db.delete(metric, timestamp, tags);
        long en = System.nanoTime();
        measure("DELETE", ist, st, en);
        _measurements.reportReturnCode("DELETE", res);
        return res;
    }

    @Override
    public int transaction(List<MyWorkload.TxParam> request) {

        return _db.transaction(request);
    }
}

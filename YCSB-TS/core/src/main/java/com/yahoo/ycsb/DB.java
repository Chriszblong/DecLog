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

import com.yahoo.ycsb.workloads.MyWorkload;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A layer for accessing a database to be benchmarked. Each thread in the client
 * will be given its own instance of whatever DB class is to be used in the test.
 * This class should be constructed using a no-argument constructor, so we can
 * load it dynamically. Any argument-based initialization should be
 * done by init().
 * <p/>
 * Note that YCSB does not make any use of the return codes returned by this class.
 * Instead, it keeps a count of the return values and presents them to the user.
 * <p/>
 * The semantics of methods such as insert, update and delete vary from database
 * to database.  In particular, operations may or may not be durable once these
 * methods commit, and some systems may return 'success' regardless of whether
 * or not a tuple with a matching key existed before the call.  Rather than dictate
 * the exact semantics of these methods, we recommend you either implement them
 * to match the database's default semantics, or the semantics of your
 * target application.  For the sake of comparison between experiments we also
 * recommend you explain the semantics you chose when presenting performance results.
 */
public abstract class DB {
    /**
     * Properties for configuring this DB.
     */
    Properties _p = new Properties();

    /**
     * Get the set of properties for this DB.
     */
    public Properties getProperties() {
        return _p;
    }

    /**
     * Set the properties for this DB.
     */
    public void setProperties(Properties p) {
        _p = p;

    }

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws DBException {
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException {
    }

    /**
     * Read a record from the database. Each value from the result will be stored in a HashMap
     *
     * @param metric    The name of the metric
     * @param timestamp The timestamp of the record to read.
     * @param tags     actual tags that were want to receive (can be empty)
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public abstract int read(String metric, Timestamp timestamp, HashMap<String, ArrayList<String>> tags);

    /**
     * Perform a range scan for a set of records in the database. Each value from the result will be stored in a HashMap.
     *
     * @param metric  The name of the metric
     * @param startTs The timestamp of the first record to read.
     * @param endTs   The timestamp of the last record to read.
     * @param tags    actual tags that were want to receive (can be empty)
     * @param avg    do averageing
     * @param sum    do summarize
     * @param count  do count
     * @param timeValue  value for timeUnit for sum/count/avg
     * @param timeUnit  timeUnit for sum/count/avg
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public abstract int scan(String metric, Timestamp startTs, Timestamp endTs, HashMap<String,
            ArrayList<String>> tags, boolean avg, boolean count, boolean sum, int timeValue,  TimeUnit timeUnit );

    /**
     * Insert a record in the database. Any tags/tagvalue pairs in the specified tags HashMap and the given value
     * will be written into the record with the specified timestamp
     *
     * @param metric    The name of the metric
     * @param timestamp The timestamp of the record to insert.
     * @param value     actual value to insert
     * @param tags      A HashMap of tag/tagvalue pairs to insert as tags
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public abstract int insert(String metric, Timestamp timestamp, double value, HashMap<String, ByteIterator> tags);

    public int update(String metric, Timestamp timestamp, double value, HashMap<String, ByteIterator> tags){
        return 0;
    }
    public int delete(String metric,Timestamp timestamp, HashMap<String, ByteIterator> tags){
        return 0;
    }

    public int transaction(List<MyWorkload.TxParam> request){
        return 0;
    }

}

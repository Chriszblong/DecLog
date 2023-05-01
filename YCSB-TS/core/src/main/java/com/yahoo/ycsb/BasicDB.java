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
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;


/**
 * Basic DB that just prints out the requested operations, instead of doing them against a database.
 */
public class BasicDB extends DB {
    public static final String VERBOSE = "basicdb.verbose";
    public static final String VERBOSE_DEFAULT = "true";

    public static final String SIMULATE_DELAY = "basicdb.simulatedelay";
    public static final String SIMULATE_DELAY_DEFAULT = "0";

    public static final String RANDOMIZE_DELAY = "basicdb.randomizedelay";
    public static final String RANDOMIZE_DELAY_DEFAULT = "true";


    boolean verbose;
    boolean randomizedelay;
    int todelay;

    public BasicDB() {
        todelay = 0;
    }


    void delay() {
        if (todelay > 0) {
            long delayNs;
            if (randomizedelay) {
                delayNs = TimeUnit.MILLISECONDS.toNanos(Utils.random().nextInt(todelay));
                if (delayNs == 0) {
                    return;
                }
            }
            else {
                delayNs = TimeUnit.MILLISECONDS.toNanos(todelay);
            }

            long now = System.nanoTime();
            final long deadline = now + delayNs;
            do {
                LockSupport.parkNanos(deadline - now);
            } while ((now = System.nanoTime()) < deadline && !Thread.interrupted());
        }
    }

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @SuppressWarnings("unchecked")
    public void init() {
        verbose = Boolean.parseBoolean(getProperties().getProperty(VERBOSE, VERBOSE_DEFAULT));
        todelay = Integer.parseInt(getProperties().getProperty(SIMULATE_DELAY, SIMULATE_DELAY_DEFAULT));
        randomizedelay = Boolean.parseBoolean(getProperties().getProperty(RANDOMIZE_DELAY, RANDOMIZE_DELAY_DEFAULT));
        if (verbose) {
            System.out.println("***************** properties *****************");
            Properties p = getProperties();
            if (p != null) {
                for (Enumeration e = p.propertyNames(); e.hasMoreElements(); ) {
                    String k = (String) e.nextElement();
                    System.out.println("\"" + k + "\"=\"" + p.getProperty(k) + "\"");
                }
            }
            System.out.println("**********************************************");
        }
    }

    /**
     * Read a record from the database. Each value from the result will be stored in a HashMap (should only be one).
     *
     * @param metric    The name of the metric
     * @param timestamp The timestamp of the record to read.
     * @param tags     actual tags that were want to receive (can be empty)
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public int read(String metric, Timestamp timestamp, HashMap<String, ArrayList<String>> tags) {
        delay();

        if (verbose) {
            System.out.print("READ " + metric + " " + timestamp + " [ ");
            System.out.print("<all fields>]");
            System.out.println("]");
        }
        if (tags.size() > 0) {
            System.out.print("[");
            for (String key : tags.keySet()) {
                System.out.print("[" + key + ":");
                for (String tagvalue : tags.get(key)) {
                    System.out.print(tagvalue+",");
                }
                System.out.print("],");
            }
            System.out.print("]");
        }
        return 0;
    }

    /**
     * Perform a range scan for a set of records in the database. Each value from the result will be stored in a HashMap.
     *
     * @param metric  The name of the metric
     * @param startTs The timestamp of the first record to read.
     * @param endTs   The timestamp of the last record to read.
     * @param tags     actual tags that were want to receive (can be empty)
     * @param avg    do averageing
     * @param sum    do summarize
     * @param count  do count
     * @param timeValue  value for timeUnit for sum/count/avg
     * @param timeUnit  timeUnit for sum/count/avg
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public int scan(String metric, Timestamp startTs, Timestamp endTs, HashMap<String,
            ArrayList<String>> tags, boolean avg, boolean count, boolean sum, int timeValue,  TimeUnit timeUnit )
    {
        delay();

        if (verbose) {
            if (avg) {
                System.out.print("AVG "  + timeValue + " " + timeUnit + " " + metric + " " + startTs + " " + endTs + " [ ");
            }
            else if (count) {
                System.out.print("COUNT " + timeValue + " " + timeUnit + " " + metric + " " + startTs + " " + endTs + " [ ");
            }
            else if (sum) {
                System.out.print("SUM "  + timeValue + " " + timeUnit + " " + metric + " " + startTs + " " + endTs + " [ ");
            }
            else {
                System.out.print("SCAN " + metric + " " + startTs + " " + endTs + " [ ");
            }
            System.out.print("<all fields>]");
            if (tags.size() > 0) {
                System.out.print("[");
                for (String key : tags.keySet()) {
                    System.out.print("[" + key + ":");
                    for (String tagvalue : tags.get(key)) {
                        System.out.print(tagvalue+",");
                    }
                    System.out.print("],");
                }
                System.out.print("]");
            }
        }

        return 0;
    }

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
    public int insert(String metric, Timestamp timestamp, double value, HashMap<String, ByteIterator> tags) {
        delay();

        if (verbose) {
            System.out.print("INSERT " + metric + " " + timestamp + " " + value + " " + " [ ");
            if (tags != null) {
                for (String k : tags.keySet()) {
                    System.out.print(k + "=" + tags.get(k) + " ");
                }
            }

            System.out.println("]");
        }

        return 0;
    }

    /**
     * Short test of BasicDB
     */
    /*
    public static void main(String[] args)
	{
		BasicDB bdb=new BasicDB();

		Properties p=new Properties();
		p.setProperty("Sky","Blue");
		p.setProperty("Ocean","Wet");

		bdb.setProperties(p);

		bdb.init();

		HashMap<String,String> fields=new HashMap<String,String>();
		fields.put("A","X");
		fields.put("B","Y");

		bdb.read("metric","key",null,null);
		bdb.insert("metric","key",fields);

		fields=new HashMap<String,String>();
		fields.put("C","Z");

		bdb.update("metric","key",fields);

		bdb.delete("metric","key");
	}*/
}

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

import com.yahoo.ycsb.workloads.CoreWorkload;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Array;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * A simple command line client to a database, using the appropriate com.yahoo.ycsb.DB implementation.
 */
public class CommandLine {
    public static final String DEFAULT_DB = "com.yahoo.ycsb.BasicDB";

    public static void usageMessage() {
        System.out.println("YCSB Command Line Client");
        System.out.println("Usage: java com.yahoo.ycsb.CommandLine [options]");
        System.out.println("Options:");
        System.out.println("  -P filename: Specify a property file");
        System.out.println("  -p name=value: Specify a property value");
        System.out.println("  -db classname: Use a specified DB class (can also set the \"db\" property)");
        System.out.println("  -metric tablename: Use the metric name instead of the default \"" + CoreWorkload.METRICNAME_PROPERTY_DEFAULT + "\"");
        System.out.println();
    }

    public static void help() {
        System.out.println("Commands:");
        System.out.println("  read key [field1 field2 ...] - Read a record");
        System.out.println("  scan key recordcount [field1 field2 ...] - Scan starting at key");
        System.out.println("  insert key name1=value1 [name2=value2 ...] - Insert a new record");
        System.out.println("  metric [tablename] - Get or [set] the name of the metric");
        System.out.println("  quit - Quit");
    }

    public static void main(String[] args) {
        int argindex = 0;

        Properties props = new Properties();
        Properties fileprops = new Properties();
        String metric = CoreWorkload.METRICNAME_PROPERTY_DEFAULT;

        while ((argindex < args.length) && (args[argindex].startsWith("-"))) {
            if ((args[argindex].compareTo("-help") == 0) ||
                    (args[argindex].compareTo("--help") == 0) ||
                    (args[argindex].compareTo("-?") == 0) ||
                    (args[argindex].compareTo("--?") == 0)) {
                usageMessage();
                System.exit(0);
            }

            if (args[argindex].compareTo("-db") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    usageMessage();
                    System.exit(0);
                }
                props.setProperty(Client.DB_PROPERTY, args[argindex]);
                argindex++;
            }
            else if (args[argindex].compareTo("-P") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    usageMessage();
                    System.exit(0);
                }
                String propfile = args[argindex];
                argindex++;

                Properties myfileprops = new Properties();
                try {
                    myfileprops.load(new FileInputStream(propfile));
                }
                catch (IOException e) {
                    System.out.println(e.getMessage());
                    System.exit(0);
                }

                for (Enumeration e = myfileprops.propertyNames(); e.hasMoreElements(); ) {
                    String prop = (String) e.nextElement();

                    fileprops.setProperty(prop, myfileprops.getProperty(prop));
                }

            }
            else if (args[argindex].compareTo("-p") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    usageMessage();
                    System.exit(0);
                }
                int eq = args[argindex].indexOf('=');
                if (eq < 0) {
                    usageMessage();
                    System.exit(0);
                }

                String name = args[argindex].substring(0, eq);
                String value = args[argindex].substring(eq + 1);
                props.put(name, value);
                //System.out.println("["+name+"]=["+value+"]");
                argindex++;
            }
            else if (args[argindex].compareTo("-metric") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    usageMessage();
                    System.exit(0);
                }
                metric = args[argindex];
                argindex++;
            }
            else {
                System.out.println("Unknown option " + args[argindex]);
                usageMessage();
                System.exit(0);
            }

            if (argindex >= args.length) {
                break;
            }
        }

        if (argindex != args.length) {
            usageMessage();
            System.exit(0);
        }

        for (Enumeration e = props.propertyNames(); e.hasMoreElements(); ) {
            String prop = (String) e.nextElement();

            fileprops.setProperty(prop, props.getProperty(prop));
        }

        props = fileprops;

        System.out.println("YCSB Command Line client");
        System.out.println("Type \"help\" for command line help");
        System.out.println("Start with \"-help\" for usage info");

        //create a DB
        String dbname = props.getProperty(Client.DB_PROPERTY, DEFAULT_DB);

        ClassLoader classLoader = CommandLine.class.getClassLoader();

        DB db = null;

        try {
            Class dbclass = classLoader.loadClass(dbname);
            db = (DB) dbclass.newInstance();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        db.setProperties(props);
        try {
            db.init();
        }
        catch (DBException e) {
            e.printStackTrace();
            System.exit(0);
        }

        System.out.println("Connected.");

        //main loop
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        for (; ; ) {
            //get user input
            System.out.print("> ");

            String input = null;

            try {
                input = br.readLine();
            }
            catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }

            if (input.compareTo("") == 0) {
                continue;
            }

            if (input.compareTo("help") == 0) {
                help();
                continue;
            }

            if (input.compareTo("quit") == 0) {
                break;
            }

            String[] tokens = input.split(" ");

            long st = System.currentTimeMillis();
            //handle commands
            if (tokens[0].compareTo("metric") == 0) {
                if (tokens.length == 1) {
                    System.out.println("Using metric \"" + metric + "\"");
                }
                else if (tokens.length == 2) {
                    metric = tokens[1];
                    System.out.println("Using metric \"" + metric + "\"");
                }
                else {
                    System.out.println("Error: syntax is \"metric tablename\"");
                }
            }
            else if (tokens[0].compareTo("read") == 0) {
                if (tokens.length < 2) {
                    System.out.println("Error: syntax is \"read timestamp [tag1=value1 tag2=value2 ...]\"");
                }
                else {
                    HashMap<String, ArrayList<String>> tags = new HashMap<String, ArrayList<String>>();

                    for (int i = 3; i < tokens.length; i++) {
                        String[] nv = tokens[i].split("=");
                        String[] values = nv[1].split(",");
                        ArrayList<String> tagValues= new ArrayList<String>();
                        for (String val : values) {
                            tagValues.add(val);
                        }
                        tags.put(nv[0], tagValues);
                    }
                    int ret = db.read(metric, new Timestamp(Long.valueOf(tokens[1])), tags);
                    System.out.println("Return code: " + ret);
//                    for (Map.Entry<Timestamp, Double> ent : results.entrySet()) {
//                        System.out.println(ent.getKey() + "=" + ent.getValue());
//                    }
                }
            }
            else if (tokens[0].compareTo("scan") == 0 || tokens[0].compareTo("avg") == 0 || tokens[0].compareTo("sum") == 0
                    || tokens[0].compareTo("count") == 0 ) {
                if (tokens.length < 3) {
                    System.out.println("Error: syntax is \"" + tokens[0] + "timestampstart timestampstop [tag1=value1,value2,... tag2=value1,.. ...]\"");
                }
                else {
                    HashMap<String, ArrayList<String>> tags = new HashMap<String, ArrayList<String>>();

                    for (int i = 3; i < tokens.length; i++) {
                        String[] nv = tokens[i].split("=");
                        String[] values = nv[1].split(",");
                        ArrayList<String> tagValues= new ArrayList<String>();
                        for (String val : values) {
                            tagValues.add(val);
                        }
                        tags.put(nv[0], tagValues);
                    }
                    boolean avg = false;
                    boolean sum = false;
                    boolean count = false;
                    if (tokens[0].compareTo("avg") == 0 ) {
                        avg = true;
                    }
                    else if (tokens[0].compareTo("count") == 0 ) {
                        count = true;
                    }
                    else if (tokens[0].compareTo("sum") == 0 ) {
                        sum = true;
                    }
                    int ret = db.scan(metric, new Timestamp(Long.valueOf(tokens[1])), new Timestamp(Long.valueOf(tokens[2])), tags, avg, count, sum, 1, TimeUnit.MILLISECONDS);
                    System.out.println("Return code: " + ret);
                    int record = 0;
//                    if (results.size() == 0) {
//                        System.out.println("0 records");
//                    }
//                    else {
//                        System.out.println("--------------------------------");
//                    }
//                    for (Map.Entry<Timestamp, Double> result : results.entrySet()) {
//                        System.out.println("Record " + (record++));
//                        System.out.println(result.getKey() + "=" + result.getValue());
//                        System.out.println("--------------------------------");
//                    }
                }
            }
            else if (tokens[0].compareTo("insert") == 0) {
                if (tokens.length < 3) {
                    System.out.println("Error: syntax is \"insert value tag1=value1 [tag2=value2 ...] (timestamp is automatically set to now)\"");
                }
                else {
                    HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

                    for (int i = 2; i < tokens.length; i++) {
                        String[] nv = tokens[i].split("=");
                        values.put(nv[0], new StringByteIterator(nv[1]));
                    }
                    try {
                        int ret = db.insert(metric, new java.sql.Timestamp(Calendar.getInstance().getTime().getTime()), Double.valueOf(tokens[1]), values);
                        System.out.println("Return code: " + ret);
                    }
                    catch (NumberFormatException e) {
                        System.out.println("Can't convert " + tokens[1] + " to double");
                    }
                }
            }
            else {
                System.out.println("Error: unknown command \"" + tokens[0] + "\"");
            }

            System.out.println((System.currentTimeMillis() - st) + " ms");

        }
    }

}

package com.yahoo.ycsb.db;

/**
 * Created by Andreas Bader on 16.10.15.
 */

import com.sun.org.apache.xpath.internal.operations.Bool;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * InfluxDB client for YCSB framework.
 * Tagfilter seems a bit problematic/not working correctly
 */
public class InfluxDBClient extends DB {
    private final int SUCCESS = 0;
    private String ip = "localhost";
    private String dbName = "testdb";
    private int port = 8086;
    private boolean _debug = false;
    private InfluxDB client;
    private boolean batch = false;
    private boolean groupBy = true;
    private boolean test = false;
    private String valueFieldName = "value"; // in which field should the value be?
    private  int batchSize = 10;

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        try {
            test = Boolean.parseBoolean(getProperties().getProperty("test", "false"));
            batch = Boolean.parseBoolean(getProperties().getProperty("batch", "false"));
            batchSize = Integer.parseInt(getProperties().getProperty("batchsize","10"));
            if (!getProperties().containsKey("port") && !test) {
                throw new DBException("No port given, abort.");
            }
            port = Integer.parseInt(getProperties().getProperty("port", String.valueOf(port)));
            dbName = getProperties().getProperty("dbName", dbName);
            if (!getProperties().containsKey("ip") && !test) {
                throw new DBException("No ip given, abort.");
            }
            ip = getProperties().getProperty("ip", ip);
            if (_debug) {
                System.out.println("The following properties are given: ");
                for (String element : getProperties().stringPropertyNames()) {
                    System.out.println(element + ": " + getProperties().getProperty(element));
                }
            }
            if (!test) {
                this.client = InfluxDBFactory.connect(String.format("http://%s:%s", ip, port), "root", "root");
                if (_debug) {
                    this.client = this.client.setLogLevel(InfluxDB.LogLevel.FULL);
                }
                if (this.batch) {
                    this.client = this.client.enableBatch(batchSize, 1000, TimeUnit.MILLISECONDS);
                }
                this.client.ping();
            }
        }
        catch (retrofit.RetrofitError e) {
            throw new DBException(String.format("Can't connect to %s:%s.)", ip, port)+e);
        }
        catch (Exception e) {
            throw new DBException(e);
        }

    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
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
    @Override
    public int read(String metric, Timestamp timestamp, HashMap<String, ArrayList<String>> tags) {
        if (metric == null || metric == "") {
            return -1;
        }
        if (timestamp == null) {
            return -1;
        }
        String tagFilter = "";
        int counter = 0;
        for (String tag : tags.keySet()) {
            tagFilter += " AND ( ";
            for (String tagname : tags.get(tag)) {
                tagFilter += String.format(" OR %s = '%s'",tag,tagname);
            }
            tagFilter = tagFilter.replaceFirst(" OR ","");
            tagFilter += " )";
        }
        // InfluxDB can not use milliseconds or nanoseconds, it uses microseconds or seconds (or greater).
        // See https://docs.influxdata.com/influxdb/v0.8/api/query_language/.
        // u stands for microseconds. Since getNanos() seems unfair because no other TSDB uses it, we just add three zeros
        Query query = new Query(String.format("SELECT * FROM %s WHERE time = %s000u%s",metric,timestamp.getTime(),tagFilter), dbName);
        if (_debug) {
            System.out.println("Query: " + query.getCommand());
        }
        if (test) {
            return SUCCESS;
        }
        QueryResult qr = this.client.query(query);
        if (qr.hasError()) {
            System.err.println("ERROR: Error occured while Querying: " + qr.getError());
            return -1;
        }
        if (qr.getResults().size() <= 0) {
            // allowed to happen!
            return -1;
        }
        for (QueryResult.Result result : qr.getResults()) {
            for (QueryResult.Series series : result.getSeries()) {
                for (List<Object> obj : series.getValues()){
                    if (Timestamp.valueOf(((String) obj.get(0)).replace("T", " ").replace("Z","")).getTime() ==
                            timestamp.getTime()) {
                        counter++;
                    }
                }
            }

        }

        if (counter == 0){
            System.err.println("ERROR: Found no values for metric: " + metric + " for timestamp: " + timestamp + ".");
            return -1;
        }
        else if (counter > 1){
            System.err.println("ERROR: Found more than one value for metric: " + metric + " for timestamp: " + timestamp + ".");
        }
        return SUCCESS;
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
    @Override
    public int scan(String metric, Timestamp startTs, Timestamp endTs, HashMap<String,
            ArrayList<String>> tags, boolean avg, boolean count, boolean sum, int timeValue, TimeUnit timeUnit ) {

        if (metric == null || metric == "") {
            return -1;
        }
        if (startTs == null || endTs == null) {
            return -1;
        }
        String tagFilter = "";
        for (String tag : tags.keySet()) {
            tagFilter += " AND ( ";
            String tempTagFilter = ""; // for OR Clauses
            for (String tagname : tags.get(tag)) {
                tempTagFilter += String.format(" OR %s = '%s'",tag,tagname);
            }
            tagFilter += tempTagFilter.replaceFirst(" OR ","");
            tagFilter += " )";
        }
        String fieldStr = "*";
        if (avg) {
            fieldStr = "MEAN("+this.valueFieldName+")";
        }
        else if (count) {
            fieldStr = "COUNT("+this.valueFieldName+")";
        }
        else if (sum) {
            fieldStr = "SUM("+this.valueFieldName+")";
        }
        String groupByStr = "";
        if (this.groupBy && timeValue != 0) {
            if (avg || count || sum) {
                groupByStr = " GROUP BY time(" + timeValue + "%s)";
                if (timeUnit == timeUnit.MILLISECONDS) {
                    groupByStr = " GROUP BY time(" + TimeUnit.MICROSECONDS.convert(timeValue, timeUnit)  + "u)";
                    System.err.println("WARNING: InfluxDB will probably not work correctly on low millisecond timeunits!");
                }
                else if (timeUnit == timeUnit.SECONDS) {
                    groupByStr = String.format(groupByStr, "s");
                }
                else if (timeUnit == timeUnit.MINUTES) {
                    groupByStr = String.format(groupByStr, "m");
                }
                else if (timeUnit == timeUnit.HOURS) {
                    groupByStr = String.format(groupByStr, "h");
                }
                else if (timeUnit == timeUnit.DAYS) {
                    groupByStr = String.format(groupByStr, "d");
                }
                else {
                    groupByStr = "GROUP BY time(" + TimeUnit.MICROSECONDS.convert(timeValue, timeUnit) + "u)";
                    System.err.println("WARNING: Unknown timeunit " + timeUnit.toString() + ", converting to milliseconds." +
                            "InfluxDB may not work correctly on low millisecond values.");
                }
            }
        }
        // InfluxDB can not use milliseconds or nanoseconds, it uses microseconds or seconds (or greater).
        // See https://docs.influxdata.com/influxdb/v0.8/api/query_language/.
        // u stands for microseconds. Since getNanos() seems unfair because no other TSDB uses it, we just add three zeros
        Query query = new Query(String.format("SELECT %s FROM %s WHERE time >= %s000u AND time <= %s000u%s%s",fieldStr,
                metric,startTs.getTime(),endTs.getTime(),tagFilter,groupByStr), dbName);
        if (_debug) {
            System.out.println("Query: " + query.getCommand());
        }
        if (test) {
            return SUCCESS;
        }
        QueryResult qr = this.client.query(query);
        if (qr.hasError()) {
            System.err.println("ERROR: Error occured while Querying: " + qr.getError() + " Query: " + query.getCommand());
            return -1;
        }
        if (qr.getResults().size() <= 0) {
            // allowed to happen!
            return -1;
        }
        Boolean found = false;
        for (QueryResult.Result result : qr.getResults()) {
            if ( result.getSeries() == null) {
                return -1;
            }
            for (QueryResult.Series series : result.getSeries()) {
                if (series.getValues().size() == 0) {
                    return -1;
                }
                for (List<Object> obj : series.getValues()){
                    // null is okay, as it means 0 for sum,count,avg
//                    if (obj.get(obj.size()-1) != null) {
                    found = true;
//                    }
                    if (obj.size() == 0) {
                        return -1;
                    }
                }
            }

        }
        if (! found) {
            return -1;
        }
        return SUCCESS;
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
    @Override
    public int insert(String metric, Timestamp timestamp, double value, HashMap<String, ByteIterator> tags) {
        if (metric == null || metric == "") {
            return -1;
        }
        if (timestamp == null) {
            return -1;
        }

        try {
            Point.Builder pb = Point.measurement(metric)
                    .time(timestamp.getTime(), TimeUnit.MILLISECONDS);
            for ( Map.Entry entry : tags.entrySet()) {
                pb = pb.field(entry.getKey().toString(), entry.getValue().toString());
            }
            pb = pb.field(this.valueFieldName, String.valueOf(value));
            if (test) {
                return SUCCESS;
            }
            // "default" = retentionPolicy
            this.client.write(this.dbName, "autogen", pb.build());
            return SUCCESS;

        } catch (Exception e) {
            System.err.println("ERROR: Error in processing insert to metric: " + metric + e);
            e.printStackTrace();
            return -1;
        }
    }

    @Override
    public int update(String metric, Timestamp timestamp, double value, HashMap<String, ByteIterator> tags) {
        return insert(metric,timestamp,value,tags);
    }
}


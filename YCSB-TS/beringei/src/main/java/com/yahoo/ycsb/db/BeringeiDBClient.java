package com.yahoo.ycsb.db;

import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.transport.TSocket;
import com.facebook.thrift.transport.TTransport;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.workloads.MyWorkload;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class BeringeiDBClient extends DB{

    private String host;
    private int port;
    private int shardCount;
    private TTransport transport;
    private BeringeiService.Client client;

    private List<DataPoint> dataPoints;
    private List<DataPoint> dataPointsToModify;
    private List<Key> keys;
    private ArrayList<TimeValuePair> timeValuePairs;
    private int timeInterval;
    private Random rand;
    private int valueMax;
    private int valueMin;
    private TxRequest txReq;


    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        rand = new Random();
        Properties properties = getProperties();
        valueMax = Integer.parseInt(properties.getProperty("valuemax","100000000"));
        valueMin = Integer.parseInt(properties.getProperty("valuemin","-100000000"));
        timeInterval =Integer.parseInt(properties.getProperty("timeinterval"));
        dataPoints = new ArrayList<>();
        for(int i = 0;i<timeInterval;i++)
            dataPoints.add( new DataPoint(new Key(),new TimeValuePair(),0));
        dataPointsToModify = new ArrayList<>();
        dataPointsToModify.add(new DataPoint(new Key(),new TimeValuePair(),0));
        keys = new ArrayList<>();
        keys.add(new Key());
        timeValuePairs = new ArrayList<>();
        host = properties.getProperty("host");
        port = Integer.parseInt(properties.getProperty("port"));
        shardCount = Integer.parseInt(properties.getProperty("shardCount"));
        transport = new TSocket(host, port);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new BeringeiService.Client(protocol);
        txReq = new TxRequest(new ArrayList<TxRequestOp>());
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        transport.close();
    }

    /**
     * Read a record from the database. Each value from the result will be stored in a HashMap
     *
     * @param metric    The name of the metric
     * @param timestamp The timestamp of the record to read.
     * @param tags      actual tags that were want to receive (can be empty)
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public int read(String metric, Timestamp timestamp, HashMap<String, ArrayList<String>> tags) {
        Map<Long, Double> updateMap;
        keys.get(0).setKey(metric);
        keys.get(0).setShardId((metric.hashCode() % shardCount + shardCount) % shardCount);
        long end = timestamp.getTime();
        long begin = timestamp.getTime();
        GetDataRequest getDataRequest = new GetDataRequest(keys,begin,end);
        GetDataResult ret = client.getData(getDataRequest);
        TimeSeriesData timeSeriesData = ret.results.get(0);
        updateMap = BeringeiUtil.createUpdateMap(timeSeriesData.data.get(0).getData(),timeSeriesData.data.get(0).getCount());
        TimeSeriesBlock timeSeriesBlock;
        for(int i = 1;i < timeSeriesData.data.size();i++){
            timeSeriesBlock = timeSeriesData.data.get(i);
            BeringeiUtil.readValues(timeValuePairs,timeSeriesBlock.data,timeSeriesBlock.count,begin,end,updateMap);
        }
        if(timeValuePairs.size() == 1 && timeValuePairs.get(0).getUnixTime() == begin){
            timeValuePairs.clear();
            return 0;
        }else{
            timeValuePairs.clear();
            return 1;
        }
    }

    /**
     * Perform a range scan for a set of records in the database. Each value from the result will be stored in a HashMap.
     *
     * @param metric    The name of the metric
     * @param startTs   The timestamp of the first record to read.
     * @param endTs     The timestamp of the last record to read.
     * @param tags      actual tags that were want to receive (can be empty)
     * @param avg       do averageing
     * @param count     do count
     * @param sum       do summarize
     * @param timeValue value for timeUnit for sum/count/avg
     * @param timeUnit  timeUnit for sum/count/avg
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    @Override
    public int scan(String metric, Timestamp startTs, Timestamp endTs, HashMap<String, ArrayList<String>> tags, boolean avg, boolean count, boolean sum, int timeValue, TimeUnit timeUnit) {
        Map<Long, Double> updateMap;
        keys.get(0).setKey(metric);
        keys.get(0).setShardId((metric.hashCode() % shardCount + shardCount) % shardCount);
        long end = endTs.getTime();
        long begin = startTs.getTime();
        GetDataRequest getDataRequest = new GetDataRequest(keys,begin,end);
        GetDataResult ret = client.getData(getDataRequest);
        TimeSeriesData timeSeriesData = ret.results.get(0);
        updateMap = BeringeiUtil.createUpdateMap(timeSeriesData.data.get(0).getData(),timeSeriesData.data.get(0).getCount());
        TimeSeriesBlock timeSeriesBlock;
        for(int i = 1;i < timeSeriesData.data.size();i++){
            timeSeriesBlock = timeSeriesData.data.get(i);
            BeringeiUtil.readValues(timeValuePairs,timeSeriesBlock.data,timeSeriesBlock.count,begin,end,updateMap);
        }
        timeValuePairs.clear();
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
    @Override
    public int insert(String metric, Timestamp timestamp, double value, HashMap<String, ByteIterator> tags) {
        DataPoint dp;
        for(int i = 0;i<timeInterval;i++){
            dp = dataPoints.get(i);
            dp.getKey().setKey(metric);
            dp.getKey().setShardId((metric.hashCode() % shardCount + shardCount) % shardCount);
            dp.getValue().setUnixTime(timestamp.getTime() + i);
            dp.getValue().setValue(getRandomDouble(rand,valueMin,valueMax));
        }
        PutDataRequest req = new PutDataRequest(dataPoints);
        PutDataResult ret = client.putDataPoints(req);
        return ret.data.size();
    }

    /**
     * Update a record in the database. Any tags/tagvalue pairs in the specified tags HashMap and the given value
     * will be written into the record with the specified timestamp
     *
     * @param metric    The name of the metric
     * @param timestamp The timestamp of the record to insert.
     * @param value     actual value to insert
     * @param tags      A HashMap of tag/tagvalue pairs to insert as tags
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    @Override
    public int update(String metric, Timestamp timestamp, double value, HashMap<String, ByteIterator> tags) {
        DataPoint dp = dataPointsToModify.get(0);
        dp.getKey().setKey(metric);
        dp.getKey().setShardId((metric.hashCode() % shardCount + shardCount) % shardCount);
        dp.getValue().setUnixTime(timestamp.getTime());
        dp.getValue().setValue(getRandomDouble(rand,valueMin,valueMax));
        UpdateDataPointsRequest req = new UpdateDataPointsRequest(dataPointsToModify);
        UpdateDataPointsResult ret = client.updateDataPoints(req);
        //System.out.println(ret);
        int status = ret.data.get(0).getStatus();
        if(status == UpdateStatusCode.OK_ALL)
            return 0;
        else
            return 1;
    }

    /**
     * Delete a record in the database. Any tags/tagvalue pairs in the specified tags HashMap and the given value
     * will be written into the record with the specified timestamp
     *
     * @param metric    The name of the metric
     * @param timestamp The timestamp of the record to insert.
     * @param tags      A HashMap of tag/tagvalue pairs to insert as tags
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    @Override
    public int delete(String metric, Timestamp timestamp, HashMap<String, ByteIterator> tags) {
        DataPoint dp = dataPointsToModify.get(0);
        dp.getKey().setKey(metric);
        dp.getKey().setShardId((metric.hashCode() % shardCount + shardCount) % shardCount);
        dp.getValue().setUnixTime(timestamp.getTime());
        DeleteDataPointsRequest req = new DeleteDataPointsRequest(dataPointsToModify);
        DeleteDataPointsResult ret = client.deleteDataPoints(req);
        int status = ret.data.get(0).getStatus();
        if(status == UpdateStatusCode.OK_ALL)
            return 0;
        else
            return 1;
    }

    /**
     * Do a transcation in the database. Any tags/tagvalue pairs in the specified tags HashMap and the given value
     * will be written into the record with the specified timestamp
     *
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    @Override
    public int transaction(List<MyWorkload.TxParam> request) {
        for(MyWorkload.TxParam p : request)
            txReq.getOp().add(new TxRequestOp(p.opCode,new Key(p.key,(p.key.hashCode() % shardCount + shardCount) % shardCount),new TimeValuePair(p.beginTime,getRandomDouble(rand,valueMin,valueMax)),0,p.beginTime,p.endTime));
        TxResult ret;
        do{
            ret = client.doTransaction(txReq);
        }while (ret.getStatus() == TxStatusCode.FAIL);
        txReq.getOp().clear();
//        if(ret.getStatus() == TxStatusCode.FAIL)
//            System.out.println("Tx fail");
        return 0;
    }


    private double getRandomDouble(Random rand, int min, int max) {
        return (double) min + (double) (max - min) * rand.nextDouble();
    }
}

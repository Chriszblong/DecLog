package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.measurements.Measurements;
import javafx.util.Pair;


import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class MyWorkload extends Workload {

    /**
     * The name of the database metric to run queries against.
     * Needs also to be set in DruidClient.java
     */
    public static final String METRICNAME_PROPERTY = "metric";

    /**
     * The default name of the database metric to run queries against.
     * Needs also to be set in DruidClient.java and H5ServClient.java
     */
    public static final String METRICNAME_PROPERTY_DEFAULT = "usermetric";

    public static final String METRIC_COUNT_PER_THREAD_PROPERTY = "metriccountperthread";

    public static final String METRIC_COUNT_PER_THREAD_PROPERTY_DEFAULT = "1";

    public static final String THREAD_COUNT_PROPERTY = "threadcount";

    public static final String THREAD_COUNT_PROPERTY_DEFAULT = "1";

    public static final String VALUE_MAX_PROPERTY = "valuemax";
    /**
     * The default maximum length of a maximum value of a time series value
     */
    public static final String VALUE_MAX_PROPERTY_DEFAULT = "10000";
    /**
     * The name of the property for the minimum value of a time series value
     */
    public static final String VALUE_MIN_PROPERTY = "valuemin";
    /**
     * The default maximum length of a minimum value of a time series value
     */
    public static final String VALUE_MIN_PROPERTY_DEFAULT = "0";

    /**
     * The name of the property for workload description
     */
    public static final String DESCRIPTION_PROPERTY = "description";
    /**
     * The default workload description
     */
    public static final String DESCRIPTION_PROPERTY_DEFAULT = "Workloaddescription";

    /**
     * The name of the property for time resolution
     */
    public static final String TIME_RESOLUTION_PROPERTY = "timeresolution";
    /**
     * The default time resolution
     */
    public static final String TIME_RESOLUTION_PROPERTY_DEFAULT = "1000";

    public static final String TIME_INTERVAL_PROPERTY = "timeinterval";

    public static final String TIME_INTERVAL_PROPERTY_DEFAULT = "1";

    /**
     * The name of the property for the proportion of transactions that are reads.
     */
    public static final String READ_PROPORTION_PROPERTY = "readproportion";

    /**
     * The default proportion of transactions that are reads.
     */
    public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.95";

    /**
     * The name of the property for the proportion of transactions that are updates.
     */
    public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";

    /**
     * The default proportion of transactions that are updates.
     */
    public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.05";

    /**
     * The name of the property for the proportion of transactions that are inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

    /**
     * The default proportion of transactions that are inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the proportion of transactions that are scans.
     */
    public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";

    /**
     * The default proportion of transactions that are scans.
     */
    public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the proportion of transactions that are read-modify-write.
     */
    public static final String READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";

    /**
     * The default proportion of transactions that are scans.
     */
    public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the distribution of requests across the keyspace. Options are
     * "uniform", "zipfian" and "latest"
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";

    /**
     * The default distribution of requests across the keyspace.
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "zipfian";

    /**
     * The name of the property for the max scan length (number of milliseconds)
     */
    public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
    /**
     * The name of the property for the min scan length (number of milliseconds)
     */
    public static final String MIN_SCAN_LENGTH_PROPERTY = "minscanlength";

    /**
     * The default max scan length.
     */
    public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "1000";
    /**
     * The default min scan length.
     */
    public static final String MIN_SCAN_LENGTH_PROPERTY_DEFAULT = "1000";

    /**
     * The name of the property for ZIPFIAN_CONSTANT
     */
    public static final String ZIPFIAN_CONSTANT_PROPERTY = "zipfianconstant";

    /**
     * The default ZIPFIAN_CONSTANT.
     */
    public static final String ZIPFIAN_CONSTANT_PROPERTY_DEFAULT = "0.99";

    /**
     * The name of the property for ZETA_CONSTANT
     */
    public static final String ZETA_CONSTANT_PROPERTY = "zetaconstant";

    /**
     * The name of the property for OPERATION_COUNT_PER_TRANSACTION_PROPERTY
     */
    public static final String OPERATION_COUNT_PER_TRANSACTION_PROPERTY = "opcountpertx";

    /**
     * The default OPERATION_COUNT_PER_TRANSACTION_PROPERTY.
     */
    public static final String OPERATION_COUNT_PER_TRANSACTION_PROPERTY_DEFAULT = "1";





    int recordCount;
    long insertStart;
    int timeInterval;
    String metric;
    int metricCntPerThread;
    int threadCnt;
    int valueMax;
    int valueMin;
    String description;
    int timeResolution;
    int[] done;
    String[] metrics;
    long[] timeStamps;
    long[] transactionTimestamps;
    int opcountpertx;

    AcknowledgedCounterGenerator transactioninsertkeysequence;
    LongGenerator keychooser;
    DiscreteGenerator operationchooser;
    String requestDistribution;
    LongGenerator scanlength;
    Measurements measurements = Measurements.getMeasurements();




    private double getRandomDouble(Random rand, int min, int max) {
        return (double) min + (double) (max - min) * rand.nextDouble();
    }

    @Override
    public void init(Properties p) throws WorkloadException {
        opcountpertx = Integer.parseInt(p.getProperty(OPERATION_COUNT_PER_TRANSACTION_PROPERTY, OPERATION_COUNT_PER_TRANSACTION_PROPERTY_DEFAULT));
        recordCount = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
        insertStart = Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
        timeInterval = Integer.parseInt(p.getProperty(TIME_INTERVAL_PROPERTY,TIME_INTERVAL_PROPERTY_DEFAULT));
        metric = p.getProperty(METRICNAME_PROPERTY, METRICNAME_PROPERTY_DEFAULT);
        metricCntPerThread = Integer.parseInt(p.getProperty(METRIC_COUNT_PER_THREAD_PROPERTY,METRIC_COUNT_PER_THREAD_PROPERTY_DEFAULT));
        threadCnt = Integer.parseInt(p.getProperty(THREAD_COUNT_PROPERTY,THREAD_COUNT_PROPERTY_DEFAULT));
        valueMax = Integer.parseInt(p.getProperty(VALUE_MAX_PROPERTY,VALUE_MAX_PROPERTY_DEFAULT));
        valueMin = Integer.parseInt(p.getProperty(VALUE_MIN_PROPERTY,VALUE_MIN_PROPERTY_DEFAULT));
        description = p.getProperty(DESCRIPTION_PROPERTY,DESCRIPTION_PROPERTY_DEFAULT);
        timeResolution = Integer.parseInt(p.getProperty(TIME_RESOLUTION_PROPERTY,TIME_RESOLUTION_PROPERTY_DEFAULT));
        requestDistribution = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY,REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
        scanlength = new UniformLongGenerator(Long.parseLong(p.getProperty(MIN_SCAN_LENGTH_PROPERTY,MIN_SCAN_LENGTH_PROPERTY_DEFAULT)),Long.parseLong(p.getProperty(MAX_SCAN_LENGTH_PROPERTY,MAX_SCAN_LENGTH_PROPERTY_DEFAULT)));
        transactioninsertkeysequence = new AcknowledgedCounterGenerator(insertStart + recordCount);
        if(requestDistribution.compareTo("zipfian") == 0){
//            final double insertproportion = Double.parseDouble(
//                    p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
            //int opcount = Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
            //int expectednewkeys = (int) ((opcount) * insertproportion * 2.0); // 2 is fudge factor
            String zeta = p.getProperty(ZETA_CONSTANT_PROPERTY);
            if(zeta!=null)
                keychooser = new ScrambledZipfianGenerator(0, recordCount * timeInterval -1, Double.parseDouble(p.getProperty(ZIPFIAN_CONSTANT_PROPERTY,ZIPFIAN_CONSTANT_PROPERTY_DEFAULT)),Double.parseDouble(zeta));
            else
                keychooser = new ScrambledZipfianGenerator(0, recordCount * timeInterval -1, Double.parseDouble(p.getProperty(ZIPFIAN_CONSTANT_PROPERTY,ZIPFIAN_CONSTANT_PROPERTY_DEFAULT)));
        } else {
            keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
        }
        operationchooser = createOperationGenerator(p);

        done = new int[threadCnt];
        metrics = new String[threadCnt * metricCntPerThread];
        timeStamps = new long[metrics.length];
        transactionTimestamps = new long[metrics.length];
        for(int i = 0;i < metrics.length;i++){
            metrics[i] = metric + i;
            timeStamps[i] = insertStart;
            transactionTimestamps[i] = insertStart + recordCount * timeInterval / metrics.length;
        }

    }

    @Override
    public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {

        return mythreadid;
    }

    @Override
    public void cleanup() throws WorkloadException {

    }

    @Override
    public boolean doInsert(DB db, Object threadstate) {
        int threadid = (Integer) threadstate;
        int metricid = threadid*metricCntPerThread + done[threadid]%metricCntPerThread;
        long timeStamp = timeStamps[metricid] * timeResolution;
        done[threadid]++;
        timeStamps[metricid] += timeInterval;
        //return db.insert(metrics[metricid],new Timestamp(timeStamp),getRandomDouble(random,valueMin,valueMax),null) == 0;
        return db.insert(metrics[metricid],new Timestamp(timeStamp),0,null) == 0;
    }

    @Override
    public boolean doTransaction(DB db, Object threadstate) {
        String operation = operationchooser.nextString();
        if(operation == null) {
            return false;
        }

        switch (operation) {
            case "READ":
                return doTransactionRead(db,threadstate);
            case "UPDATE":
                return doTransactionUpdate(db,threadstate);
            case "INSERT":
                return doTransactionInsert(db,threadstate);
            case "SCAN":
                return doTransactionScan(db,threadstate);
            case "READMODIFYWRITE":
                return doTransactionReadModifyWrite(db,threadstate);
        }
        return false;
    }
    private Pair<String,Long> getMetricAndTime(long keynum){
        int metricid = (int)(keynum % (metrics.length));
        long timestamp = keynum / metrics.length + insertStart;
        return new Pair<>(metrics[metricid],timestamp);
    }



    public boolean doTransactionRead(DB db, Object threadstate) {
        // choose a random key
        long keynum;
        Pair<String,Long> metricTime;
        List<TxParam> op = new ArrayList<>();

        for(int i=0;i<opcountpertx;i++){
            keynum = keychooser.nextLong();
            metricTime = getMetricAndTime(keynum);
            op.add(new TxParam(1,metricTime.getKey(),0,metricTime.getValue(),0));
        }

        long ist = measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();

        int ret = db.transaction(op);

        long en = System.nanoTime();

        measurements.measure("READ", (int) ((en - st) / 1000));
        measurements.measureIntended("READ", (int) ((en - ist) / 1000));
        measurements.reportReturnCode("READ", ret);



        return ret == 0;

    }

    public boolean doTransactionReadModifyWrite(DB db, Object threadstate) {
        // choose a random key
        long keynum;
        Pair<String,Long> metricTime;
        List<TxParam> op = new ArrayList<>();

        for(int i=0;i<opcountpertx;i++){
            keynum = keychooser.nextLong();
            metricTime = getMetricAndTime(keynum);
            op.add(new TxParam(1,metricTime.getKey(),0,metricTime.getValue(),0));
        }

        long ist = measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();

        int ret = db.transaction(op);

        long en = System.nanoTime();

        measurements.measure("READ-MODIFY-WRITE", (int) ((en - st) / 1000));
        measurements.measureIntended("READ-MODIFY-WRITE", (int) ((en - ist) / 1000));
        measurements.reportReturnCode("READ-MODIFY-WRITE", ret);


        return ret == 0;
    }

    public boolean doTransactionScan(DB db, Object threadstate) {
        // choose a random key
        long keynum = keychooser.nextLong();
        Pair<String,Long> metricTime = getMetricAndTime(keynum);
        long len;
        List<TxParam> op = new ArrayList<>();
        for(int i=0;i<opcountpertx;i++){
            keynum = keychooser.nextLong();
            metricTime = getMetricAndTime(keynum);
            len = scanlength.nextLong();
            op.add(new TxParam(2,metricTime.getKey(),0,metricTime.getValue(),metricTime.getValue() + len));
        }

        long ist = measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();

        int ret = db.transaction(op);

        long en = System.nanoTime();

        measurements.measure("SCAN", (int) ((en - st) / 1000));
        measurements.measureIntended("SCAN", (int) ((en - ist) / 1000));
        measurements.reportReturnCode("SCAN", ret);


        return ret == 0;
    }

    public boolean doTransactionUpdate(DB db, Object threadstate) {
        // choose a random key
        long keynum;
        Pair<String,Long> metricTime;
        List<TxParam> op = new ArrayList<>();

        for(int i=0;i<opcountpertx;i++){
            keynum = keychooser.nextLong();
            metricTime = getMetricAndTime(keynum);
            op.add(new TxParam(3,metricTime.getKey(),0,metricTime.getValue(),0));
        }

        long ist = measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();

        int ret = db.transaction(op);

        long en = System.nanoTime();

        measurements.measure("UPDATE", (int) ((en - st) / 1000));
        measurements.measureIntended("UPDATE", (int) ((en - ist) / 1000));
        measurements.reportReturnCode("UPDATE", ret);

        return ret == 0;
    }

    public boolean doTransactionInsert(DB db, Object threadstate) {
        int threadid = (Integer) threadstate;
        int metricid;
        long timeStamp;
        List<TxParam> op = new ArrayList<>();

        for(int i=0;i<opcountpertx;i++){
            metricid = threadid*metricCntPerThread + done[threadid]%metricCntPerThread;
            done[threadid]++;
            timeStamp = transactionTimestamps[metricid] * timeResolution;
            transactionTimestamps[metricid] += timeInterval;
            op.add(new TxParam(0,metrics[metricid],0,timeStamp,0));
        }

        long ist = measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();

        int ret = db.transaction(op);

        long en = System.nanoTime();

        measurements.measure("INSERT", (int) ((en - st) / 1000));
        measurements.measureIntended("INSERT", (int) ((en - ist) / 1000));
        measurements.reportReturnCode("INSERT", ret);


        return ret == 0;
    }


    static DiscreteGenerator createOperationGenerator(final Properties p) {
        if (p == null) {
            throw new IllegalArgumentException("Properties object cannot be null");
        }
        final double readproportion = Double.parseDouble(
                p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
        final double updateproportion = Double.parseDouble(
                p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
        final double insertproportion = Double.parseDouble(
                p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
        final double scanproportion = Double.parseDouble(
                p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
        final double readmodifywriteproportion = Double.parseDouble(p.getProperty(
                READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));

        final DiscreteGenerator operationchooser = new DiscreteGenerator();
        if (readproportion > 0) {
            operationchooser.addValue(readproportion, "READ");
        }

        if (updateproportion > 0) {
            operationchooser.addValue(updateproportion, "UPDATE");
        }

        if (insertproportion > 0) {
            operationchooser.addValue(insertproportion, "INSERT");
        }

        if (scanproportion > 0) {
            operationchooser.addValue(scanproportion, "SCAN");
        }

        if (readmodifywriteproportion > 0) {
            operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
        }
        return operationchooser;
    }

    public static class TxParam {
        public int opCode;
        public String key;
        public double value;
        public long beginTime;
        public long endTime;

        public TxParam(int opCode, String key, double value, long beginTime, long endTime) {
            this.opCode = opCode;
            this.key = key;
            this.value = value;
            this.beginTime = beginTime;
            this.endTime = endTime;
        }
    }

}

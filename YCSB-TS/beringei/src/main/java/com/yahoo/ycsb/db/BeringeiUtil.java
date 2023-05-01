package com.yahoo.ycsb.db;

import com.facebook.thrift.protocol.*;
import com.facebook.thrift.transport.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BeringeiUtil {

    private static final long kDefaultDelta = 60;
    private static final int kBitsForFirstTimestamp = 31;
    private static final int kLeadingZerosLengthBits = 5;
    private static final int kBlockSizeLengthBits = 6;
    private static final int kBlockSizeAdjustment = 1;
    private static final Map<String,Integer>[] timestampEncodings = new HashMap[4];
    private static final long FLAGS_gorilla_blacklisted_time_min = 0;
    private static final long FLAGS_gorilla_blacklisted_time_max = 0;


    static {
        timestampEncodings[0] = new HashMap<String,Integer>(){{put("bitsForValue",7);put("controlValue",2);put("controlValueBitLength",2);}};
        timestampEncodings[1] = new HashMap<String,Integer>(){{put("bitsForValue",9);put("controlValue",6);put("controlValueBitLength",3);}};
        timestampEncodings[2] = new HashMap<String,Integer>(){{put("bitsForValue",12);put("controlValue",14);put("controlValueBitLength",4);}};
        timestampEncodings[3] = new HashMap<String,Integer>(){{put("bitsForValue",32);put("controlValue",15);put("controlValueBitLength",4);}};
    }

    /*private static class {
        int64_t bitsForValue;
        uint32_t controlValue;
        uint32_t controlValueBitLength;
    } static const timestampEncodings[4] = {{7, 2, 2},
        {9, 6, 3},
        {12, 14, 4},
        {32, 15, 4}};*/


    public static long readValueFromBitString(
    byte[] bitString,
    Param param,
    int bitsToRead) {
        long value = 0;
        for (int i = 0; i < bitsToRead; i++) {
            value <<= 1;
            long bit = (bitString[(int)(param.bitPos >> 3)] >> (7 - (param.bitPos & 0x7))) & 1;
            value += bit;
            param.bitPos++;
        }

        return value;
    }

    public static double readNextValue(
    byte[] data,
    Param param) {
        long nonZeroValue = readValueFromBitString(data, param, 1);

        if (nonZeroValue == 0) {

            return Double.longBitsToDouble(param.previousValue);
        }

        long usePreviousBlockInformation =
                readValueFromBitString(data, param, 1);

        long xorValue;
        if (usePreviousBlockInformation != 0) {
            xorValue = readValueFromBitString(
                    data, param, (int)(64 - param.previousLeadingZeros - param.previousTrailingZeros));
            xorValue <<= param.previousTrailingZeros;
        } else {
            long leadingZeros =
                    readValueFromBitString(data, param, kLeadingZerosLengthBits);
            long blockSize =
                    readValueFromBitString(data, param, kBlockSizeLengthBits) +
                    kBlockSizeAdjustment;

            param.previousTrailingZeros = 64 - blockSize - leadingZeros;
            xorValue = readValueFromBitString(data, param, (int)blockSize);
            xorValue <<= param.previousTrailingZeros;
            param.previousLeadingZeros = leadingZeros;
        }

        long value = xorValue ^ param.previousValue;
        param.previousValue = value;

        return Double.longBitsToDouble(value);
    }

    public static int findTheFirstZeroBit(
    byte[] bitString,
    Param param,
    int limit) {
        int bits = 0;

        while (bits < limit) {
            long bit = readValueFromBitString(bitString, param, 1);
            if (bit == 0) {
                return bits;
            }

            bits++;
        }

        return bits;
    }

    public static long readNextTimestamp(
    byte[] data,
    Param param) {
        int type = findTheFirstZeroBit(data, param, 4);
        if (type > 0) {
            // Delta of delta is non zero. Calculate the new delta. `index`
            // will be used to find the right length for the value that is
            // read.
            int index = type - 1;
            long decodedValue = readValueFromBitString(
                    data, param, timestampEncodings[index].get("bitsForValue"));

            // [0,255] becomes [-128,127]
            decodedValue -=
                    ((long)1 << (timestampEncodings[index].get("bitsForValue") - 1));
            if (decodedValue >= 0) {
                // [-128,127] becomes [-128,128] without the zero in the middle
                decodedValue++;
            }

            param.previousTimestampDelta += decodedValue;
        }

        param.previousTimestamp += param.previousTimestampDelta;
        return param.previousTimestamp;
    }
    private static class Param{
        public long previousValue = 0;
        public long previousLeadingZeros = 0;
        public long previousTrailingZeros = 0;
        public long bitPos = 0;
        public long previousTimestamp = 0;
        public long previousTimestampDelta = kDefaultDelta;
    }

    public static Map<Long,Double> createUpdateMap(byte[] data, int n){
        Param p = new Param();
        long time, value;
        Map<Long,Double> ret = new HashMap<>();
        for(int i=0;i<n;i++){
            time = readValueFromBitString(data, p,64);
            value = readValueFromBitString(data, p,64);
            ret.put(time, Double.longBitsToDouble(value));
        }
        return ret;
    }

    public static int readValues(ArrayList<TimeValuePair> out,
                                 byte[] data,
                                 int n,
                                 long begin,
                                 long end,
                                 Map<Long,Double> updateMap){
            long tombstone;
            if (data.length == 0 || n == 0) {
                return 0;
            }
            out.ensureCapacity(out.size() + n);

            Param param = new Param();

            long firstTimestamp = readValueFromBitString(
                    data, param, kBitsForFirstTimestamp);

            Double firstValue = readNextValue(
                    data,
                    param);

            //**********************
            tombstone = readValueFromBitString(data, param, 1);
            param.bitPos++;
            //**********************

            param.previousTimestamp = firstTimestamp;

            // If the first data point is after the query range, return nothing.
            if (firstTimestamp > end) {
                return 0;
            }

            int count = 0;
            if (firstTimestamp >= begin) {

                if(tombstone == 1){
                    firstValue = updateMap.get(firstTimestamp);
                    if(firstValue != null) {
                        out.add(new TimeValuePair(firstTimestamp, firstValue));
                        count++;
                    }
                }else {
                    out.add(new TimeValuePair(firstTimestamp, firstValue));
                    count++;
                }

            }

            long unixTime;
            Double value;
            for (int i = 1; i < n; i++) {
                unixTime = readNextTimestamp(
                        data, param);
                value = readNextValue(
                        data,
                        param);

                //**********************
                tombstone = readValueFromBitString(data, param, 1);
                param.bitPos++;
                //**********************

                if (unixTime > end) {
                    break;
                }

                if (unixTime >= begin) {
                    if (unixTime < FLAGS_gorilla_blacklisted_time_min ||
                            unixTime > FLAGS_gorilla_blacklisted_time_max) {

                        if(tombstone == 1){
                            value = updateMap.get(unixTime);
                            if(value != null) {
                                out.add(new TimeValuePair(unixTime, value));
                                count++;
                            }
                        }else {
                            out.add(new TimeValuePair(unixTime, value));
                            count++;
                        }

                    }
                }
            }

            return count;
    }
    public static void getDataPoints(long begin,long end){
        TTransport transport = new TSocket("192.168.164.43", 9998);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        BeringeiService.Client client = new BeringeiService.Client(protocol);

        List<Key> keys = new ArrayList<>();
        keys.add(new Key("usermetric0",("usermetric0".hashCode()%4 + 4) %4));
//        long end = System.currentTimeMillis()/1000;
//        long begin = end - 3*60*60;
        GetDataRequest getDataRequest = new GetDataRequest(keys,begin,end);
        GetDataResult ret = client.getData(getDataRequest);
        transport.close();
        Map<Long, Double> updateMap;
        for(int i = 0; i<ret.results.size(); i++){
            TimeSeriesData timeSeriesData = ret.results.get(i);
            //System.out.println(timeSeriesData);
            updateMap = BeringeiUtil.createUpdateMap(timeSeriesData.data.get(i).getData(), timeSeriesData.data.get(i).getCount());
            ArrayList<TimeValuePair> timeValuePairs = new ArrayList<>();
            TimeSeriesBlock timeSeriesBlock;
            for(int j = 1;j < timeSeriesData.data.size();j++){
                timeSeriesBlock = timeSeriesData.data.get(j);
                BeringeiUtil.readValues(timeValuePairs,timeSeriesBlock.data,timeSeriesBlock.count,begin,end,updateMap);
            }
            for(TimeValuePair timeValuePair:timeValuePairs){
                System.out.print(keys.get(i).key + "：");
                System.out.println(timeValuePair);
            }
        }
        System.out.println(ret);
    }

    public static void updateDataPoints(DataPoint dataPoint){
        TTransport transport = new TSocket("192.168.164.43", 9998);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        BeringeiService.Client client = new BeringeiService.Client(protocol);
        List<DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(dataPoint);

        UpdateDataPointsResult ret = client.updateDataPoints(new UpdateDataPointsRequest(dataPoints));

        transport.close();
        System.out.println(ret);

    }

    public static void putDataPoints(DataPoint dataPoint){
        TTransport transport = new TSocket("192.168.164.43", 9998);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        BeringeiService.Client client = new BeringeiService.Client(protocol);
        //DataPoint dp = new DataPoint(new Key("test",91),new TimeValuePair(0,1.25),0);
        List<DataPoint> dataPointList = new ArrayList<>();
        dataPointList.add(dataPoint);
        PutDataResult ret = client.putDataPoints(new PutDataRequest(dataPointList));
        transport.close();
        System.out.println(ret);
    }
    public static void deleteDataPoints(DataPoint dataPoint){
        TTransport transport = new TSocket("192.168.164.43", 9998);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        BeringeiService.Client client = new BeringeiService.Client(protocol);
        //DataPoint dp = new DataPoint(new Key("test",91),new TimeValuePair(0,1.25),0);
        List<DataPoint> dataPointList = new ArrayList<>();
        dataPointList.add(dataPoint);
        DeleteDataPointsResult ret = client.deleteDataPoints(new DeleteDataPointsRequest(dataPointList));
        transport.close();
        System.out.println(ret);
    }

    public static void doTransaction(List<TxRequestOp> ops){
        TTransport transport = new TSocket("192.168.164.43", 9998);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        BeringeiService.Client client = new BeringeiService.Client(protocol);
        //DataPoint dp = new DataPoint(new Key("test",91),new TimeValuePair(0,1.25),0);
        TxRequest req = new TxRequest(ops);
        TxResult ret = client.doTransaction(req);
        transport.close();
        System.out.println(ret);
    }

    public static void main(String[] args) {
        //putDataPoints();
        //getDataPoints();
//        for(int i=0;i<1000;i++)
//            System.out.println((((("usermetric" + i).hashCode()) % 4)+4)%4);
        //System.out.println(-9%3);
        DataPoint dataPoint = new DataPoint(new Key("usermetric",(("usermetric".hashCode()%8 + 8) %8)),new TimeValuePair(1000000,4.5),0);
        /*for(int i = 1000;i<5000;i++){
            updateDataPoints(new DataPoint(new Key("usermetric",(("usermetric".hashCode()%8 + 8) %8)),new TimeValuePair(1000000 + i,1.5),0));
        }*/
        List<TxRequestOp> ops = new ArrayList<>();
//        ops.add(new TxRequestOp(TxOpCode.INSERT,new Key("usermetric",(("usermetric".hashCode()%8 + 8) %8)),new TimeValuePair(1000000,1.25),0,0,0));
//        ops.add(new TxRequestOp(TxOpCode.INSERT,new Key("usermetric",(("usermetric".hashCode()%8 + 8) %8)),new TimeValuePair(1000001,1.25),0,0,0));
//        ops.add(new TxRequestOp(TxOpCode.INSERT,new Key("usermetric",(("usermetric".hashCode()%8 + 8) %8)),new TimeValuePair(1000003,1.25),0,0,0));

        //ops.add(new TxRequestOp(TxOpCode.SCAN,new Key("usermetric",(("usermetric".hashCode()%8 + 8) %8)),new TimeValuePair(1000000,1.25),0,1000000,1000100));
        ops.add(new TxRequestOp(TxOpCode.INSERT,new Key("/checkpoint",(("usermetric".hashCode()%8 + 8) %8)),new TimeValuePair(1000000,18.5),0,0,0));
//        ops.add(new TxRequestOp(TxOpCode.INSERT,new Key("usermetric",(("usermetric".hashCode()%8 + 8) %8)),new TimeValuePair(1000003,16.5),0,0,0));
//        ops.add(new TxRequestOp(TxOpCode.INSERT,new Key("usermetric",(("usermetric".hashCode()%8 + 8) %8)),new TimeValuePair(1000004,1.25),0,0,0));
//        ops.add(new TxRequestOp(TxOpCode.INSERT,new Key("usermetric",(("usermetric".hashCode()%8 + 8) %8)),new TimeValuePair(1000005,1.5),0,0,0));
//        ops.add(new TxRequestOp(TxOpCode.READ,new Key("usermetric",(("usermetric".hashCode()%8 + 8) %8)),new TimeValuePair(1000003,1.25),0,0,0));
//        ops.add(new TxRequestOp(TxOpCode.UPDATE,new Key("usermetric",(("usermetric".hashCode()%8 + 8) %8)),new TimeValuePair(1000004,4.5),0,0,0));
//        ops.add(new TxRequestOp(TxOpCode.READ,new Key("usermetric",(("usermetric".hashCode()%8 + 8) %8)),new TimeValuePair(1000005,1.25),0,0,0));
        //ops.add(new TxRequestOp(TxOpCode.DELETE,new Key("usermetric",(("usermetric".hashCode()%8 + 8) %8)),new TimeValuePair(1000003,10.5),0,0,0));
        //putDataPoints(dataPoint);
        //updateDataPoints(dataPoint);
        //deleteDataPoints(dataPoint);
        //doTransaction(ops);
        getDataPoints(1438000800,1438000810);
        //System.out.println((("usermetric".hashCode()%8 + 8) %8));

    }

}

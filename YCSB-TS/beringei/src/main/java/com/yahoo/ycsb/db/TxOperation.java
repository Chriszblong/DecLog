package com.yahoo.ycsb.db;

public class TxOperation {
    public static final int INSERT = 0;
    public static final int READ = 1;
    public static final int SCAN = 2;
    public static final int UPDATE = 3;
    public static final int DELETE = 4;
    public int opcode;
    public String key;
    public long shardId;
    public long time;
    public double value;
    public int categoryId;
    public long startTime;
    public long endTime;
}

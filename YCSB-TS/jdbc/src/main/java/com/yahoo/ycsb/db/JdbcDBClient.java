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

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced with YCSB.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 *
 * <br> Each client will have its own instance of this class. This client is
 * not thread safe.
 *
 * <br> This interface expects a schema <key> <field1> <field2> <field3> ...
 * All attributes are of type VARCHAR. All accesses are through the primary key. Therefore, 
 * only one index on the primary key is needed.
 *
 * <p> The following options must be passed when using this database client.
 *
 * <ul>
 * <li><b>db.driver</b> The JDBC driver class to use.</li>
 * <li><b>db.url</b> The Database connection URL.</li>
 * <li><b>db.user</b> User name for the connection.</li>
 * <li><b>db.passwd</b> Password for the connection.</li>
 * </ul>
 *
 * @author sudipto
 *
 */
public class JdbcDBClient extends DB implements JdbcDBClientConstants {

    private static final String DEFAULT_PROP = "";
    private ArrayList<Connection> conns;
    private boolean initialized = false;
    private Properties props;
    private Integer jdbcFetchSize;
    private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;
    private boolean test = false;
    private boolean doGroupBy = true;

    /**
     * For the given key, returns what shard contains data for this key
     *
     * @param key Data key to do operation on
     * @return Shard index
     */
    private int getShardIndexByKey(Timestamp key) {
        int ret = Math.abs(String.valueOf(key.getTime()).hashCode()) % conns.size();
        //System.out.println(conns.size() + ": Shard instance for "+ key + " (hash  " + key.hashCode()+ " ) " + " is " + ret);
        return ret;
    }

    /**
     * For the given key, returns Connection object that holds connection
     * to the shard that contains this key
     *
     * @param key Data key to get information for
     * @return Connection object
     */
    private Connection getShardConnectionByKey(Timestamp key) {
        return conns.get(getShardIndexByKey(key));
    }

    private void cleanupAllConnections() throws SQLException {
        for (Connection conn : conns) {
            conn.close();
        }
    }

    /**
     * Initialize the database connection and set it up for sending requests to the database.
     * This must be called once per client.
     * @throws
     */
    @Override
    public void init() throws DBException {
        if (initialized) {
            System.err.println("Client connection already initialized.");
            return;
        }
        test = Boolean.parseBoolean(getProperties().getProperty("test", "false"));
        // doGroupBy should be on by default, because otherwise this vioaltes SQL standard
        // MySQL and PostgreSQL still work, but MonetDB not
        // As the PK is unique, there can't be any double values
        // which means there can't be undefined results .. -> MySQL/PostgreSQL is correct
        // But the Group By should be there says SQL ->
        // https://www.monetdb.org/pipermail/users-list/2014-October/007555.html
        // https://www.monetdb.org/pipermail/users-list/2013-June/006462.html
        //
        doGroupBy = Boolean.parseBoolean(getProperties().getProperty("dogroupby", "true"));
        props = getProperties();
        String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
        String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
        String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
        String driver = props.getProperty(DRIVER_CLASS);

        String jdbcFetchSizeStr = props.getProperty(JDBC_FETCH_SIZE);
        if (jdbcFetchSizeStr != null) {
            try {
                this.jdbcFetchSize = Integer.parseInt(jdbcFetchSizeStr);
            }
            catch (NumberFormatException nfe) {
                System.err.println("Invalid JDBC fetch size specified: " + jdbcFetchSizeStr);
                throw new DBException(nfe);
            }
        }

        String autoCommitStr = props.getProperty(JDBC_AUTO_COMMIT, Boolean.TRUE.toString());
        Boolean autoCommit = Boolean.parseBoolean(autoCommitStr);

        try {
            if (driver != null) {
                Class.forName(driver);
            }
            int shardCount = 0;
            conns = new ArrayList<Connection>(3);
            if (!test) {
                for (String url : urls.split(",")) {
                    System.out.println("Adding shard node URL: " + url);
                    Connection conn = DriverManager.getConnection(url, user, passwd);

                    // Since there is no explicit commit method in the DB interface, all
                    // operations should auto commit, except when explicitly told not to
                    // (this is necessary in cases such as for PostgreSQL when running a
                    // scan workload with fetchSize)
                    conn.setAutoCommit(autoCommit);

                    shardCount++;
                    conns.add(conn);
                }
            }
            else {
                conns.add(null);
            }
            System.out.println("Using " + shardCount + " shards");

            cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();
        }
        catch (ClassNotFoundException e) {
            System.err.println("Error in initializing the JDBS driver: " + e);
            throw new DBException(e);
        }
        catch (SQLException e) {
            System.err.println("Error in database operation: " + e);
            throw new DBException(e);
        }
        catch (NumberFormatException e) {
            System.err.println("Invalid value for fieldcount property. " + e);
            throw new DBException(e);
        }
        initialized = true;
    }

    @Override
    public void cleanup() throws DBException {
        try {
            if (!test) {
                cleanupAllConnections();
            }
        }
        catch (SQLException e) {
            System.err.println("Error in closing the connection. " + e);
            throw new DBException(e);
        }
    }

    private void generateTagString(HashMap<String, ArrayList<String>> tags, StringBuilder sb) {
        int counter=0;
        int tagCounter=0;
        if (tags.keySet().size() > 0) {
            sb.append("(");
            for (String tag : tags.keySet()) {
                if (tags.get(tag).size() > 0) {
                    sb.append("( ");
                    for (String tagValue : tags.get(tag)) {
                        sb.append(tag + " = " + "'" + tagValue + "' ");
                        sb.append("OR ");
                        counter++;
                    }
                    sb.delete(sb.lastIndexOf("OR"), sb.lastIndexOf("OR")+2);
                    sb.append(") ");
                    // If its the last one...
                    if (tagCounter < tags.keySet().size() -1) {
                        sb.append("AND ");
                    }
                    tagCounter++;
                }
            }
            sb.append(")");
        }
        if (counter > 0) {
            sb.append(" AND ");
        }
    }

    private PreparedStatement createAndCacheInsertStatement(StatementType insertType, Timestamp key, String Columns)
            throws SQLException {
        StringBuilder insert = new StringBuilder("INSERT INTO ");
        insert.append(insertType.tableName);
        insert.append(Columns);
        insert.append(" VALUES(?");
        for (int i = 1; i < insertType.numFields; i++) {
            insert.append(",?");
        }
        insert.append(");");
        PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(insert.toString());
        PreparedStatement stmt = cachedStatements.putIfAbsent(insertType, insertStatement);
        if (stmt == null) return insertStatement;
        else return stmt;
    }

    private PreparedStatement createAndCacheReadStatement(StatementType readType, Timestamp key, HashMap<String, ArrayList<String>> tags)
            throws SQLException {
        StringBuilder read = new StringBuilder("SELECT * FROM ");
        read.append(readType.tableName);
        read.append(" WHERE ");
        generateTagString(tags, read);
        read.append(" ( ");
        read.append(TIMESTAMP_KEY);
        read.append(" = ");
        read.append("?);");
        PreparedStatement readStatement = getShardConnectionByKey(key).prepareStatement(read.toString());
//        PreparedStatement stmt = cachedStatements.putIfAbsent(readType, readStatement);
//        if (stmt == null) return readStatement;
//        else return stmt;
        return readStatement;
    }

    private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType, Timestamp key)
            throws SQLException {
        StringBuilder delete = new StringBuilder("DELETE FROM ");
        delete.append(deleteType.tableName);
        delete.append(" WHERE ");
        delete.append(TIMESTAMP_KEY);
        delete.append(" = ?;");
        PreparedStatement deleteStatement = getShardConnectionByKey(key).prepareStatement(delete.toString());
        PreparedStatement stmt = cachedStatements.putIfAbsent(deleteType, deleteStatement);
        if (stmt == null) return deleteStatement;
        else return stmt;
    }

    private PreparedStatement createAndCacheScanStatement(StatementType scanType, Timestamp key, HashMap<String, ArrayList<String>> tags,
            boolean avg, boolean count, boolean sum, boolean ms)
            throws SQLException {
        String selectStr = "*";
        String groupByStr = "";
        if (avg) {
            if (! ms) {
                selectStr = "AVG(VALUE) as VALUE";
            }
            else {
                selectStr = TIMESTAMP_KEY + ", AVG(VALUE) as VALUE";
                if (doGroupBy) {
                    groupByStr = "GROUP BY " + TIMESTAMP_KEY;
                }
            }
        }
        else if (count) {
            if (! ms) {
                selectStr = "COUNT(*) as VALUE";
            }
            else {
                selectStr = TIMESTAMP_KEY + ", COUNT(*) as VALUE";
                if (doGroupBy) {
                    groupByStr = "GROUP BY " + TIMESTAMP_KEY;
                }
            }
        }
        else if (sum) {
            if (! ms) {
                selectStr = "SUM(VALUE) as VALUE";
            }
            else {
                selectStr = TIMESTAMP_KEY + ", SUM(VALUE) as VALUE";
                if (doGroupBy) {
                    groupByStr = "GROUP BY " + TIMESTAMP_KEY;
                }
            }
        }
        StringBuilder select = new StringBuilder("SELECT " + selectStr + " FROM ");
        select.append(scanType.tableName);
        select.append(" WHERE ");
        generateTagString(tags, select);
        select.append(TIMESTAMP_KEY);
        select.append(" BETWEEN ? AND ? ");
        select.append(groupByStr + ";");
        PreparedStatement scanStatement = getShardConnectionByKey(key).prepareStatement(select.toString());
        if (this.jdbcFetchSize != null) scanStatement.setFetchSize(this.jdbcFetchSize);
//        PreparedStatement stmt = cachedStatements.putIfAbsent(scanType, scanStatement);
//        if (stmt == null) return scanStatement;
//        else return stmt;
        return scanStatement;
    }

    /**
     * Read a record from the database. Each value from the result will be stored in a HashMap .
     *
     * @param metric    The name of the metric
     * @param timestamp The timestamp of the record to read.
     * @param tags     actual tags that were want to receive (can be empty)
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public int read(String metric, Timestamp timestamp, HashMap<String, ArrayList<String>> tags) {
        if (metric == null) {
            return -1;
        }
        if (timestamp == null) {
            return -1;
        }
        try {
            StatementType type = new StatementType(StatementType.Type.READ, metric, 1, getShardIndexByKey(timestamp));
            PreparedStatement readStatement = cachedStatements.get(type);
            if (test) {
                return SUCCESS;
            }
            readStatement = createAndCacheReadStatement(type, timestamp,tags);
            readStatement.setTimestamp(1, timestamp);
            ResultSet resultSet = readStatement.executeQuery();
            if (!resultSet.next()) {
                resultSet.close();
                return -1;
            }
//            if (values != null) {
//                double value = resultSet.getDouble("VALUE");
//                values.put(timestamp, value);
//            }
            resultSet.close();
            return SUCCESS;
        }
        catch (SQLException e) {
            System.err.println("ERROR: Error in processing read of table " + metric + ": " + e);
            return -1;
        }
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
            ArrayList<String>> tags, boolean avg, boolean count, boolean sum, int timeValue,  TimeUnit timeUnit ) {
        if (metric == null) {
            return -1;
        }
        if (startTs == null || endTs == null) {
            return -1;
        }
        try {
            StatementType type = new StatementType(StatementType.Type.SCAN, metric, 1, getShardIndexByKey(startTs));
            PreparedStatement scanStatement = cachedStatements.get(type);
            boolean ms = false;
            if (timeValue != 0) {
                if (TimeUnit.MILLISECONDS.convert(timeValue, timeUnit) == 1) {
                    ms = true;
                }
                else {
                    System.err.println("WARNING: JDBC does not support granularity, defaulting to one bucket.");
                }
            }
            if (test) {
                return SUCCESS;
            }
            scanStatement = createAndCacheScanStatement(type, startTs, tags, avg, count, sum, ms);
            scanStatement.setTimestamp(1, startTs);
            scanStatement.setTimestamp(2, endTs);
            ResultSet resultSet = scanStatement.executeQuery();
            if (!resultSet.next()) {
                resultSet.close();
                return -1;
            }
//            while (resultSet.next()) {
//                if (values != null) {
//                    try {
//                        values.put(resultSet.getTimestamp(TIMESTAMP_KEY), resultSet.getDouble("VALUE"));
//                    }
//                    catch (Exception e) {
//                        System.out.println("ERROR: While processing scan results PK: " + TIMESTAMP_KEY + " VALUE: " + resultSet.getDouble("VALUE") + ".");
//                        System.out.println(e);
//                    }
//                }
//            }
            resultSet.close();
            return SUCCESS;
        }
        catch (SQLException e) {
            System.err.println("ERROR: Error in processing scan of table: " + metric + e);
            return -1;
        }
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
        if (metric == null) {
            return -1;
        }
        if (timestamp == null) {
            return -1;
        }
        try {
            int numFields = tags.size()+2; // +PK +VALUE
            StatementType type = new StatementType(StatementType.Type.INSERT, metric, numFields, getShardIndexByKey(timestamp));
            PreparedStatement insertStatement = cachedStatements.get(type);
            String columns = "(" + TIMESTAMP_KEY + ",VALUE";
            for (Map.Entry<String, ByteIterator> entry : tags.entrySet()) {
                String tagname = entry.getKey().toString();
                columns+=","+tagname;
            }
            columns+=")";
            if (test) {
                return SUCCESS;
            }
            if (insertStatement == null) {
                insertStatement = createAndCacheInsertStatement(type, timestamp, columns);
            }
            insertStatement.setTimestamp(1, timestamp);
            insertStatement.setDouble(2, value);
            int index = 3;
            for (Map.Entry<String, ByteIterator> entry : tags.entrySet()) {
                String tagvalue = entry.getValue().toString();
                insertStatement.setString(index++, tagvalue);
            }
            int result = insertStatement.executeUpdate();

            if (result == 1) return SUCCESS;
            else return -1;
        }
        catch (SQLException e) {
            System.err.println("Error in processing insert to table: " + metric + e);
            return -1;
        }
    }

    /**
     * The statement type for the prepared statements.
     */
    private static class StatementType {

        Type type;
        int shardIndex;
        int numFields;
        String tableName;

        StatementType(Type type, String tableName, int numFields, int _shardIndex) {
            this.type = type;
            this.tableName = tableName;
            this.numFields = numFields;
            this.shardIndex = _shardIndex;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + numFields + 100 * shardIndex;
            result = prime * result
                    + ((tableName == null) ? 0 : tableName.hashCode());
            result = prime * result + ((type == null) ? 0 : type.getHashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            StatementType other = (StatementType) obj;
            if (numFields != other.numFields)
                return false;
            if (shardIndex != other.shardIndex)
                return false;
            if (tableName == null) {
                if (other.tableName != null)
                    return false;
            }
            else if (!tableName.equals(other.tableName))
                return false;
            if (type != other.type)
                return false;
            return true;
        }

        enum Type {
            INSERT(1),
            DELETE(2),
            READ(3),
            UPDATE(4),
            SCAN(5),;
            int internalType;

            private Type(int type) {
                internalType = type;
            }

            int getHashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + internalType;
                return result;
            }
        }
    }
}

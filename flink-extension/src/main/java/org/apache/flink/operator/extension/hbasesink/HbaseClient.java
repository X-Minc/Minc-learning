package org.apache.flink.operator.extension.hbasesink;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @Author: Minc
 * @DateTime: 2022/8/17
 */
public class HbaseClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseClient.class);
    private static Connection connection = null;

    /**
     * 检查连接，如果未创建则创建链接
     */
    public static Boolean create(Configuration configuration) throws Exception {
        if (Objects.isNull(connection) || connection.isClosed()) {
            connection = ConnectionFactory.createConnection(configuration);
            return true;
        } else {
            return false;
        }
    }

    public static Boolean checkCreated() throws Exception {
        return Objects.nonNull(connection) && !connection.isClosed();
    }

    public static Connection getConnection() {
        return connection;
    }

    /**
     * 检查连接，如果连接未关闭则关闭链接
     */
    public static Boolean close() throws Exception {
        if (connection.isClosed()) {
            return false;
        } else {
            connection.close();
            return true;
        }
    }

    public static Boolean checkClose() throws Exception {
        return !connection.isClosed();
    }

    public static Admin getAdmin() throws Exception {
        return connection.getAdmin();
    }

    public static Boolean createTable(String tableName, String family, Integer ttl) throws Exception {
        Admin admin = getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
            hTableDescriptor.addFamily(hColumnDescriptor);
            if (ttl != null) {
                hColumnDescriptor.setTimeToLive(ttl);
            }
            admin.createTable(hTableDescriptor);
            return true;
        } else {
            return false;
        }

    }

    public static Table getTable(String tableName) throws Exception {
        return connection.getTable(TableName.valueOf(tableName));
    }

    public static Result[] getQuery(Table table, Get... query) throws Exception {
        return table.get(Arrays.asList(query));
    }

    public static ResultScanner scanQuery(Table table, Scan scan) throws Exception {
        return table.getScanner(scan);
    }


    public static void putQuery(Table table, Put... put) throws Exception {
        table.put(Arrays.asList(put));
    }
}

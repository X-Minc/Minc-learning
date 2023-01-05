package org.apache.flink.operator.extension.jdbcsink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.operator.extension.DatabaseEnum;
import org.apache.flink.operator.extension.MetricsUtil;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.util.*;

/**
 * 自定义jdbcSink
 *
 * @Author: Minc
 * @DateTime: 2022.6.14 0014
 */
public class JdbcSink<T> extends RichSinkFunction<T> implements Serializable, CheckpointedFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSink.class);
    private Boolean autoCommit = true;
    private Integer splitSize = 1000;
    private Boolean split = false;
    //connection pool
    private DruidDataSource druidDataSource;
    private DatabaseEnum database = DatabaseEnum.mysql;
    private String operateName = "jdbcSink";
    //common sql
    private String sql;
    //transform T to required struct
    private JdbcSinkAdapter<T> jdbcSinkAdapter;
    //druid properties
    private Properties properties;
    private Meter InTps;
    private Integer slotIndex;
    private ListState<T> batchState;


    public static <T> JdbcSinkBuilder<T> builder() {
        return new JdbcSinkBuilder<>();
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        Preconditions.checkNotNull(sql, "Sql must not be null!");
        Preconditions.checkNotNull(jdbcSinkAdapter, "ValueGetter must not be null!");
        Preconditions.checkNotNull(properties, "Druid properties must not be null!");
        InTps = MetricsUtil.registerInTPSMeter(MetricsUtil.MetricsGroup.sink, getRuntimeContext());
        LOGGER.info("operateName {},database {},sql {},slot {}", operateName, database, sql, slotIndex);
        Properties druidPro = getDruidPro(properties);
        druidDataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(druidPro);
    }

    private Properties getDruidPro(Properties properties) {
        Properties pro = new Properties();
        String driver = properties.getProperty(database + "." + DruidDataSourceFactory.PROP_DRIVERCLASSNAME);
        String url = properties.getProperty(database + "." + DruidDataSourceFactory.PROP_URL);
        String username = properties.getProperty(database + "." + DruidDataSourceFactory.PROP_USERNAME);
        String password = properties.getProperty(database + "." + DruidDataSourceFactory.PROP_PASSWORD);
        String initialSize = properties.getProperty(database + "." + DruidDataSourceFactory.PROP_INITIALSIZE);
        String maxActive = properties.getProperty(database + "." + DruidDataSourceFactory.PROP_MAXACTIVE);
        String minIdle = properties.getProperty(database + "." + DruidDataSourceFactory.PROP_MINIDLE);
        String maxWait = properties.getProperty(database + "." + DruidDataSourceFactory.PROP_MAXWAIT);
        Preconditions.checkNotNull(driver, database + ".driver must not be null!");
        Preconditions.checkNotNull(url, database + ".url must not be null!");
        Preconditions.checkNotNull(username, database + ".username must not be null!");
        Preconditions.checkNotNull(password, database + ".username must not be null!");
        pro.setProperty(DruidDataSourceFactory.PROP_DRIVERCLASSNAME, driver);
        pro.setProperty(DruidDataSourceFactory.PROP_URL, url);
        pro.setProperty(DruidDataSourceFactory.PROP_USERNAME, username);
        pro.setProperty(DruidDataSourceFactory.PROP_PASSWORD, password);
        if (Objects.nonNull(initialSize)) pro.setProperty(DruidDataSourceFactory.PROP_DRIVERCLASSNAME, driver);
        if (Objects.nonNull(maxActive)) pro.setProperty(DruidDataSourceFactory.PROP_MAXACTIVE, maxActive);
        if (Objects.nonNull(minIdle)) pro.setProperty(DruidDataSourceFactory.PROP_MINIDLE, minIdle);
        if (Objects.nonNull(maxWait)) pro.setProperty(DruidDataSourceFactory.PROP_MAXWAIT, maxWait);
        return pro;
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        InTps.markEvent();
        batchState.add(value);
    }

    @Override
    public void close() throws Exception {
        LOGGER.info("operateName {},Slot {},jdbcSink {} closed", operateName, slotIndex, operateName);
        druidDataSource.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        //防止触发savepoint途中checkpoint正在进行
        try {
            DruidPooledConnection connection = druidDataSource.getConnection();
            connection.setAutoCommit(autoCommit);
            int num = 0;
            try {
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                for (T t : batchState.get()) {
                    Object[] values = jdbcSinkAdapter.getValues(t);
                    for (int i = 1; i <= values.length; i++) {
                        preparedStatement.setObject(i, values[i - 1]);
                    }
                    preparedStatement.addBatch();
                    num++;
                    if (split && (num % splitSize == 0)) {
                        preparedStatement.executeBatch();
                        preparedStatement.clearBatch();
                    }
                }
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
                if (!autoCommit) {
                    connection.commit();
                }
                preparedStatement.close();
            } catch (Exception e) {
                connection.rollback();
                throw new Exception(e);
            } finally {
                if (!autoCommit) {
                    connection.setAutoCommit(true);
                }
                connection.close();
            }
            batchState.clear();
            LOGGER.info("operateName {} ,slot {},flush successful ! size {}", this.operateName, slotIndex, num);
        } catch (Exception e) {
            LOGGER.error("operateName {} ,slot {},flush failed ! reason: ", this.operateName, slotIndex, e);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        slotIndex = getRuntimeContext().getIndexOfThisSubtask();
        ListStateDescriptor<T> batchStateDescriptor = new ListStateDescriptor<>("batchState", (Class<T>) Object.class);
        batchState = functionInitializationContext.getOperatorStateStore().getListState(batchStateDescriptor);
    }

    /*
     * builder
     */
    public static class JdbcSinkBuilder<T> {
        private final JdbcSink<T> jdbcSink;

        public JdbcSinkBuilder() {
            this.jdbcSink = new JdbcSink<>();
        }

        public JdbcSinkBuilder<T> setSql(String sql) {
            this.jdbcSink.sql = sql;
            return this;
        }

        public JdbcSinkBuilder<T> setSplit(Boolean split) {
            this.jdbcSink.split = split;
            return this;
        }

        public JdbcSinkBuilder<T> setSplitSize(Integer splitSize) {
            this.jdbcSink.splitSize = splitSize;
            return this;
        }

        public JdbcSinkBuilder<T> setJdbcSinkAdapter(JdbcSinkAdapter<T> jdbcSinkAdapter) {
            this.jdbcSink.jdbcSinkAdapter = jdbcSinkAdapter;
            return this;
        }

        public JdbcSinkBuilder<T> setOperateName(String operateName) {
            this.jdbcSink.operateName = operateName;
            return this;
        }

        public JdbcSinkBuilder<T> setDatabase(DatabaseEnum database) {
            this.jdbcSink.database = database;
            return this;
        }

        public JdbcSinkBuilder<T> setAutoCommit(Boolean autoCommit) {
            this.jdbcSink.autoCommit = autoCommit;
            return this;
        }

        public JdbcSinkBuilder<T> setProperties(Properties properties) {
            this.jdbcSink.properties = properties;
            return this;
        }

        public JdbcSink<T> build() {
            return this.jdbcSink;
        }
    }
}

package org.apache.flink.operator.extension.hbasesink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.operator.extension.MetricsUtil;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * @Author: Minc
 * @DateTime: 2022/7/5
 */
public class HbaseSink<IN> extends RichSinkFunction<IN> implements Serializable, CheckpointedFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseSink.class);
    private String operateName;
    private Properties conf;
    private DataTransform<IN> transform;
    private BufferedMutator bufferedMutator;
    private Integer slotIndex;
    private Meter InTps;
    private ListState<Put> batchState;


    public static <IN> HbaseSinkBuilder<IN> builder() {
        return new HbaseSinkBuilder<>(new HbaseSink<>());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        InTps = MetricsUtil.registerInTPSMeter(MetricsUtil.MetricsGroup.sink, getRuntimeContext());
        org.apache.hadoop.conf.Configuration configuration = getConfiguration(conf);
        if (!HbaseClient.checkCreated()) {
            HbaseClient.create(configuration);
        }
        Preconditions.checkNotNull(conf.get("hbase.table.name"), "Please set 'hbase.table.name' in configuration ! ");
        String name = conf.getProperty("hbase.table.name");
        Preconditions.checkNotNull(conf.get("hbase.table.family"), "Please set 'hbase.table.family' in configuration ! ");
        String family = conf.getProperty("hbase.table.family");
        if (HbaseClient.createTable(name, family, null)) {
            LOGGER.info("table {} create successful !", name);
        } else {
            LOGGER.info("table {} has existed !", name);
        }
        TableName tableName = TableName.valueOf(name);
        BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        LOGGER.info("hbase.table.name is {}", params.getTableName().getNameAsString());
        bufferedMutator = HbaseClient.getConnection().getBufferedMutator(params);
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        InTps.markEvent();
        Put put = transform.doTransform(value, conf);
        if (Objects.nonNull(put)) {
            batchState.add(put);
        }
    }

    private void flush(List<Put> data) throws IOException {
        bufferedMutator.mutate(data);
        bufferedMutator.flush();
    }

    @Override
    public void close() throws Exception {
        if (!HbaseClient.checkClose()) {
            HbaseClient.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        try {
            List<Put> data = new ArrayList<>();
            int size = 0;
            for (Put put : batchState.get()) {
                data.add(put);
                size++;
            }
            LOGGER.info("operateName {},slot {},current batch size is {}", operateName, slotIndex, size);
            flush(data);
            batchState.clear();
            LOGGER.info("operateName {},slot {},HbaseSink ,flush successful ! ", operateName, slotIndex);
        } catch (Exception e) {
            LOGGER.error("operateName {},slot {},HbaseSink ,flush failed ! reason: ", operateName, slotIndex, e);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        slotIndex = getRuntimeContext().getIndexOfThisSubtask();
        ListStateDescriptor<Put> batchStateDescriptor = new ListStateDescriptor<>("batchState", Put.class);
        batchState = context.getOperatorStateStore().getListState(batchStateDescriptor);
        if (context.isRestored()) {
            LOGGER.info("HbaseSink ,recover state successful ! slot {}", slotIndex);
        } else {
            LOGGER.info("HbaseSink ,nothing to recover ! slot {}", slotIndex);
        }
    }

    public static class HbaseSinkBuilder<IN> {
        private final HbaseSink<IN> hbaseSink;

        public HbaseSinkBuilder(HbaseSink<IN> hbaseSink) {
            this.hbaseSink = hbaseSink;
        }

        public HbaseSinkBuilder<IN> setOperateName(String operateName) {
            this.hbaseSink.operateName = operateName;
            return this;
        }

        public HbaseSinkBuilder<IN> setConf(Properties conf) {
            this.hbaseSink.conf = conf;
            return this;
        }

        public HbaseSinkBuilder<IN> setTransform(DataTransform<IN> transform) {
            this.hbaseSink.transform = transform;
            return this;
        }

        public HbaseSink<IN> build() {
            return this.hbaseSink;
        }
    }

    private org.apache.hadoop.conf.Configuration getConfiguration(Properties conf) {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        for (Map.Entry<Object, Object> entry : conf.entrySet()) {
            configuration.set(entry.getKey().toString(), entry.getValue().toString());
        }
        return configuration;
    }
}

package minc.hudi.compact;

import minc.hudi.AvroUtil;
import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.compact.CompactFunction;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionCommitSink;
import org.apache.hudi.sink.compact.CompactionPlanSourceFunction;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.StreamerUtil;

public class Compaction {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(1000L * 20);
    env.setParallelism(1);
    Configuration compactConf = new Configuration();
    compactConf.setString(FlinkOptions.TABLE_TYPE.key(), "MERGE_ON_READ");
    compactConf.setInteger(FlinkOptions.COMPACTION_TASKS.key(), 1);
    compactConf.setString(FlinkOptions.PATH.key(), "/link");
    compactConf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS.key(), 1);
    compactConf.setBoolean(FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key(), false);
    compactConf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), false);
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(compactConf);
    compactConf.setString(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());
    CompactionUtil.setAvroSchema(compactConf, metaClient);
    compactConf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA,
        Schema.createRecord("link_node", "null", "link_nodes", false, AvroUtil.getWriteSchema())
            .toString());
    compactConf.setBoolean(FlinkOptions.CHANGELOG_ENABLED, false);
    HoodieFlinkWriteClient writeClient = StreamerUtil.createWriteClient(compactConf);
    Option<String> compactionInstantTimeOption = CompactionUtil.getCompactionInstantTime(
        metaClient);
    writeClient.scheduleCompactionAtInstant(compactionInstantTimeOption.get(), Option.empty());
    String compactionInstantTime = compactionInstantTimeOption.get();
    HoodieFlinkTable<?> table = writeClient.getHoodieTable();
    HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
        table.getMetaClient(), compactionInstantTime);
    HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    // Mark instant as compaction inflight
    table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);
    env.addSource(new CompactionPlanSourceFunction(compactionPlan, compactionInstantTime))
        .name("compaction_source")
        .uid("uid_compaction_source")
        .rebalance()
        .transform("compact_task",
            TypeInformation.of(CompactionCommitEvent.class),
            new ProcessOperator<>(new CompactFunction(compactConf)))
        .setParallelism(compactionPlan.getOperations().size())
        .addSink(new CompactionCommitSink(compactConf))
        .name("clean_commits")
        .uid("uid_clean_commits")
        .setParallelism(1);
    env.execute();
  }
}

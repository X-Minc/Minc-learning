package minc.hudi.write.kafka;

import java.util.Properties;
import minc.hudi.PropertiesUtil;
import minc.hudi.Udf;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: Minc
 * @DateTime: 2022/11/8
 */
public class SqlFromKafka {

  public static void main(String[] args) throws Exception {
    Properties properties = (Properties) PropertiesUtil.getProperties().get("kafka");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
    tableEnvironment.createTemporarySystemFunction("getAlias", Udf.AliasFunction.class);
    tableEnvironment.createTemporarySystemFunction("getTime", Udf.GetTimeFunction.class);
    tableEnvironment.createTemporarySystemFunction("getPartition",
        Udf.GetTimePartitionFunction.class);
    env.enableCheckpointing(1000L * 20);
    env.setParallelism(1);
    tableEnvironment.executeSql("create table link_nodes\n" +
        "(\n" +
        "\t`traceId` STRING,\n" +
        "\t`duration` BIGINT,\n" +
        "\t`tracerType` STRING,\n" +
        "\t`id` STRING,\n" +
        "\t`parentId` STRING,\n" +
        "\t`timestamp` bigint,\n" +
        "\t`tags` STRING\n" +
        ")with(\n" +
        "\t'connector'='kafka',\n" +
        "\t'topic'='" + properties.getProperty("kafka.topic") + "',\n" +
        "\t'properties.bootstrap.servers'='"+properties.getProperty("kafka.bootstrap")+"',\n" +
        "\t'properties.group.id'='"+properties.getProperty("kafka.groupId")+"',\n" +
        "\t'scan.startup.mode'='latest-offset',\n" +
        "\t'format'='json'\n" +
        ")");
    tableEnvironment.executeSql("create table link_nodes_hudi\n" +
        "(\n" +
        "\t`traceId` STRING,\n" +
        "\t`duration` BIGINT,\n" +
        "\t`tracerType` STRING,\n" +
        "\t`id` STRING,\n" +
        "\t`parentId` STRING,\n" +
        "\t`timestamp` bigint,\n" +
        "\t`tags` STRING ,\n" +
        "\tPRIMARY KEY (traceId,id) NOT ENFORCED\n" +
        ")with\n" +
        "(\n" +
        " 'connector'='hudi',\n" +
        " 'write.tasks'='1',\n" +
        " 'compaction.tasks'='1',\n" +
        " 'compaction.delta_commits'='1',\n" +
        " 'hoodie.datasource.write.recordkey.field'='traceId.id',\n" +
        " 'table.type' = 'MERGE_ON_READ',\n" +
        " 'path'='oss://hudi-minc/link_node'\n" +
        ")");
    tableEnvironment.executeSql("insert into link_nodes_hudi select * from link_nodes");
    env.execute();
  }
}

package minc.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: Minc
 * @DateTime: 2022/11/10
 */
public class HudiSqlRead {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L * 20);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        env.setParallelism(1);
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
                " 'read.tasks'='1',\n" +
                " 'table.type' = 'MERGE_ON_READ',\n" +
                " 'path'='oss://hudi-minc/link_nodes_hudi'\n" +
                ")");
        tableEnvironment.executeSql("select * from link_nodes_hudi").print();
        env.execute();
    }
}

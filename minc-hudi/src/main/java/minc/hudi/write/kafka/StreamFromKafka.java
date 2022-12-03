package minc.hudi.write.kafka;

import com.alibaba.fastjson.JSONObject;
import java.util.Properties;
import minc.hudi.write.DbSchema;
import minc.hudi.AvroUtil;
import minc.hudi.PropertiesUtil;
import org.apache.avro.Schema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class StreamFromKafka {

  public static void main(String[] args) throws Exception {
    Properties properties = (Properties) PropertiesUtil.getProperties().get("kafka");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(1000L * 20);
    env.setParallelism(1);
    KafkaSource<JSONObject> kafkaSource = KafkaSource
        .<JSONObject>builder()
        .setBootstrapServers(properties.getProperty("kafka.bootstrap"))
        .setTopics(properties.getProperty("kafka.topic"))
        .setGroupId(properties.getProperty("kafka.groupId"))
        .setValueOnlyDeserializer(new DbSchema())
        .setStartingOffsets(OffsetsInitializer.latest())
        .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        .build();

    DataStreamSource<JSONObject> test = env
        .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

    RowType rowType = AvroUtil.getWriteRowType();

    JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
        rowType,
        InternalTypeInfo.of(rowType),
        false,
        true,
        TimestampFormat.ISO_8601
    );

    SingleOutputStreamOperator<RowData> rowDataSingleOutputStreamOperator = test
        .map(x -> JSONObject.toJSONString(x))
        .map(x -> deserializationSchema.deserialize(x.getBytes(StandardCharsets.UTF_8.name())));

    List<Schema.Field> fields = AvroUtil.getWriteSchema();

    //writer configuration
    Configuration writeConf = new Configuration();
    writeConf.setString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "rowKey");
    writeConf.setString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "pt");
    writeConf.setString(FlinkOptions.PRECOMBINE_FIELD.key(), "pt");
    writeConf.setString(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ");
    writeConf.setInteger(FlinkOptions.BUCKET_ASSIGN_TASKS.key(), 1);
    writeConf.setInteger(FlinkOptions.WRITE_TASKS.key(), 1);
    writeConf.setString(FlinkOptions.TABLE_NAME.key(), "link");
    writeConf.setBoolean(FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key(), false);
    writeConf.setString(FlinkOptions.PATH.key(), "oss://hudi-minc/link");
    writeConf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA.key(),
        Schema.createRecord("link_node", "null", "link_nodes", false, fields).toString());

    DataStream<HoodieRecord> bootstrap = Pipelines.bootstrap(writeConf, rowType, 1,
        rowDataSingleOutputStreamOperator);
    DataStream<Object> pipeline = Pipelines.hoodieStreamWrite(writeConf, 1, bootstrap);
    env.addOperator(pipeline.getTransformation());
    env.execute();
  }
}

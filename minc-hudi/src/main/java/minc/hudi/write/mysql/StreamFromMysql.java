package minc.hudi.write.mysql;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import minc.hudi.PropertiesUtil;
import org.apache.avro.Schema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.sink.utils.Pipelines;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Author: Minc
 * @DateTime: 2022/11/11
 */
public class StreamFromMysql {

  public static void main(String[] args) throws Exception {
    Properties pro = (Properties) PropertiesUtil.getProperties().get("mysql");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(1000L * 20);
    env.setParallelism(1);
    Properties properties = new Properties();
    properties.put("snapshot.mode", "initial");
    MySqlSource<String> mysqlCdcSource = MySqlSource
        .<String>builder()
        .hostname(pro.getProperty("mysql.hostname"))
        .username(pro.getProperty("mysql.username"))
        .password(pro.getProperty("mysql.password"))
        .databaseList(pro.getProperty("mysql.database"))
        .tableList(pro.getProperty("mysql.table"))
        .debeziumProperties(properties)
        .deserializer(new JsonDebeziumDeserializationSchema())
        .build();
    RowType rowType = RowType.of(
        new LogicalType[]{new VarCharType(), new IntType(), new VarCharType()},
        new String[]{"name", "age", "gender"});
    JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
        rowType,
        InternalTypeInfo.of(rowType),
        false,
        true,
        TimestampFormat.ISO_8601
    );
    SingleOutputStreamOperator<RowData> rowDataSingleOutputStreamOperator = env
        .fromSource(mysqlCdcSource, WatermarkStrategy.noWatermarks(), "mysqlCdcTest")
        .map(x -> JSONObject.parseObject(x).getString("after"))
        .map(x -> deserializationSchema.deserialize(x.getBytes(StandardCharsets.UTF_8)));

    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("name", Schema.create(Schema.Type.STRING)));
    fields.add(new Schema.Field("age", Schema.create(Schema.Type.INT)));
    fields.add(new Schema.Field("gender", Schema.create(Schema.Type.STRING)));

    Configuration configuration = new Configuration();
    configuration.setString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "name");
    configuration.setString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "age");
    configuration.setString(FlinkOptions.PRECOMBINE_FIELD.key(), "gender");
    configuration.setBoolean(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, false);
    configuration.setString(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ");
    configuration.setInteger(FlinkOptions.BUCKET_ASSIGN_TASKS.key(), 1);
    configuration.setInteger(FlinkOptions.WRITE_TASKS.key(), 1);
    configuration.setString(FlinkOptions.TABLE_NAME.key(), "student");
    configuration.setString(FlinkOptions.PATH.key(), "oss://hudi-minc/student_hudi");
    configuration.setString(FlinkOptions.SOURCE_AVRO_SCHEMA.key(),
        Schema.createRecord("student", "null", "example.arvo", false, fields).toString());

    DataStream<HoodieRecord> bootstrap = Pipelines.bootstrap(configuration, rowType, 1,
        rowDataSingleOutputStreamOperator);
    DataStream<Object> pipeline = Pipelines.hoodieStreamWrite(configuration, 1, bootstrap);
    env.addOperator(pipeline.getTransformation());

    env.execute();
  }
}

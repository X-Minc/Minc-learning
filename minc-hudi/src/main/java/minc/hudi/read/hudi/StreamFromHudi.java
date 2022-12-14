package minc.hudi.read.hudi;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import minc.hudi.AvroUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTableSource;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

public class StreamFromHudi {

  public static void main(String[] args) throws Exception {
    String basePath = "/link/";
    Configuration conf = getDefaultConf(basePath);
    HoodieTableSource tableSource = new HoodieTableSource(
        AvroUtil.getReadTableStruct(),
        new Path(basePath),
        Arrays.asList(conf.getString(FlinkOptions.PARTITION_PATH_FIELD).split(",")),
        "",
        conf);
    Path[] readPaths = tableSource.getReadPaths();
    HoodieTableMetaClient metaClient = tableSource.getMetaClient();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withEngineType(EngineType.FLINK)
        .withPath(basePath).build();
    HoodieFlinkTable table = HoodieFlinkTable.create(config, HoodieFlinkEngineContext.DEFAULT,
        metaClient);
    for (Path readPath : readPaths) {
      String name = readPath.getName();
      table.getFileSystemView()
          .getAllFileGroups(name).forEach(baseFile -> {
                List<HoodieBaseFile> collect = baseFile.getAllBaseFiles().collect(Collectors.toList());
            baseFile.getAllBaseFiles().forEach(x -> {
              Path path = new Path(x.getPath());
              try {
                ParquetReader<GenericRecord> reader = AvroParquetReader
                    .<GenericRecord>builder(path)
                    .build();
                GenericRecord nextRecord = reader.read();
                while (nextRecord != null) {
                  nextRecord = reader.read();
                  if (Objects.nonNull(nextRecord))
                    System.out.println(nextRecord);
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
            });
          });
    }
  }

  public static Configuration getDefaultConf(String tablePath) throws Exception {
    Configuration conf = new Configuration();
    conf.setString(FlinkOptions.PATH, tablePath);
    conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA,
        Schema.createRecord("link_node", "null", "link_nodes", false, AvroUtil.getWriteSchema())
            .toString());
    conf.setString(FlinkOptions.TABLE_NAME, "link");
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, "pt");
    conf.setBoolean(FlinkOptions.READ_AS_STREAMING, true);
    return conf;
  }

}

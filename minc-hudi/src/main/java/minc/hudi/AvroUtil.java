package minc.hudi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.PhysicalColumn;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;

public class AvroUtil {

  public static List<Schema.Field> getWriteSchema() throws Exception {
    Schema tagsSchema = Schema.createRecord("tags", "", "tags", false);
    List<Field> tagsFields = new ArrayList<>();
    tagsFields.add(new Field("request_url", Schema.create(Type.STRING)));
    tagsFields.add(new Field("artifactId", Schema.create(Type.STRING)));
    Schema header = Schema.createMap(Schema.create(Type.STRING));
    tagsFields.add(new Field("request_headers", header));
    tagsSchema.setFields(tagsFields);
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("rowKey", Schema.create(Type.STRING)));
    fields.add(new Schema.Field("pt", Schema.create(Type.STRING)));
    fields.add(new Schema.Field("traceId", Schema.create(Type.STRING)));
    fields.add(new Schema.Field("id", Schema.create(Type.STRING)));
    fields.add(new Schema.Field("parentId", Schema.create(Type.STRING)));
    fields.add(new Schema.Field("tracerType", Schema.create(Type.STRING)));
    fields.add(new Schema.Field("duration", Schema.create(Type.LONG)));
    fields.add(new Schema.Field("domain", Schema.create(Type.STRING)));
    fields.add(new Schema.Field("timestamp", Schema.create(Type.LONG)));
    fields.add(new Field("tags", tagsSchema));
    return fields;
  }

  public static ResolvedSchema getReadTableStruct() {
    List<Column> columns = new ArrayList<>();
    PhysicalColumn rowKey = Column.physical("rowKey", new AtomicDataType(new VarCharType()));
    PhysicalColumn pt = Column.physical("pt", new AtomicDataType(new VarCharType()));
    PhysicalColumn traceId = Column.physical("traceId", new AtomicDataType(new VarCharType()));
    PhysicalColumn id = Column.physical("id", new AtomicDataType(new VarCharType()));
    PhysicalColumn parentId = Column.physical("parentId", new AtomicDataType(new VarCharType()));
    PhysicalColumn tracerType = Column.physical("tracerType",
        new AtomicDataType(new VarCharType()));
    PhysicalColumn duration = Column.physical("duration", new AtomicDataType(new BigIntType()));
    PhysicalColumn domain = Column.physical("domain", new AtomicDataType(new VarCharType()));
    PhysicalColumn timestamp = Column.physical("timestamp", new AtomicDataType(new BigIntType()));
    List<RowField> tagsFields = getTagsRowType();
    PhysicalColumn tags = Column.physical("tagsFields",
        new AtomicDataType(new RowType(tagsFields)));
    columns.add(rowKey);
    columns.add(pt);
    columns.add(traceId);
    columns.add(id);
    columns.add(parentId);
    columns.add(tracerType);
    columns.add(duration);
    columns.add(domain);
    columns.add(timestamp);
    columns.add(tags);
    ArrayList<WatermarkSpec> watermarkSpecs = new ArrayList<>();
    watermarkSpecs.add(WatermarkSpec.of("timestamp", new ValueLiteralExpression("error")));
    return new ResolvedSchema(columns, watermarkSpecs, UniqueConstraint.primaryKey("rowKey",
        Arrays.asList(new String[]{"rowKey"})));
  }

  public static RowType getWriteRowType() throws Exception {
    List<RowField> tags = getTagsRowType();
    return RowType.of(
        new LogicalType[]{new VarCharType(), new VarCharType(), new VarCharType(),
            new VarCharType(), new VarCharType(), new VarCharType(), new BigIntType(),
            new VarCharType(), new BigIntType(), new RowType(tags)},
        new String[]{"rowKey", "pt", "traceId", "id", "parentId", "tracerType",
            "duration", "domain", "timestamp", "tags"});
  }

  public static List<RowField> getTagsRowType() {
    List<RowField> tags = new ArrayList<>();
    tags.add(new RowField("request_url", new VarCharType()));
    tags.add(new RowField("artifactId", new VarCharType()));
    MapType mapType = new MapType(new VarCharType(), new VarCharType());
    tags.add(new RowField("request_headers", mapType));
    return tags;
  }


}

package minc.hudi.write;

import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import minc.hudi.DateTransformUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class DbSchema implements DeserializationSchema<JSONObject> {

  @Override
  public TypeInformation<JSONObject> getProducedType() {
    return TypeInformation.of(JSONObject.class);
  }

  @Override
  public JSONObject deserialize(byte[] message) throws IOException {
    JSONObject jsonObject = JSONObject.parseObject(new String(message));
    String environment = jsonObject.getString("env");
    String pt = DateTransformUtil.getStringDateAfterOffsetFromDate(
        jsonObject.getLong("timestamp") / 1000,
        "yyyyMM",
        Duration.ZERO);
    jsonObject.put("pt", pt);
    JSONObject tags = jsonObject.getJSONObject("tags");
    String parentId = jsonObject.getString("parentId");
    if (Objects.isNull(parentId)) {
      jsonObject.put("parentId", "null");
    }
    String traceId = jsonObject.getString("traceId");
    String id = jsonObject.getString("id");
    jsonObject.put("rowKey", traceId + "." + id);
    String header = (String) tags.remove("request.headers");
    JSONObject object;
    if (Objects.nonNull(header)) {
      object = JSONObject.parseObject(header);
      String client = (String) object.remove("x-client-id");
      String token = (String) object.remove("x-token");
      object.put("client_id", client);
      object.put("token", token);
    } else {
      object = new JSONObject();
      object.put("client_id", "null");
      object.put("token", "null");
    }
    tags.put("request_headers", object);
    String artifactId = tags.getString("artifactId");
    if (Objects.isNull(artifactId)) {
      tags.put("artifactId", "null");
    }
    String url = (String) tags.remove("request.url");
    if (Objects.nonNull(url)) {
      tags.put("request_url", url);
    } else {
      tags.put("request_url", "null");
    }
    return jsonObject;
  }

  @Override
  public boolean isEndOfStream(JSONObject nextElement) {
    return false;
  }
}

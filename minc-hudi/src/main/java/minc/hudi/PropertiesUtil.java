package minc.hudi;

import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import minc.hudi.write.kafka.StreamFromKafka;

public class PropertiesUtil {

  public static Properties getProperties() throws Exception {
    Properties result = new Properties();
    Properties properties = new Properties();
    properties.load(StreamFromKafka.class.getClassLoader()
        .getResourceAsStream("config.properties"));
    for (Entry<Object, Object> entry : properties.entrySet()) {
      String key = entry.getKey().toString();
      String parentKey = key.substring(0,
          key.lastIndexOf(".") == -1 ? key.length() + 1 : key.lastIndexOf("."));
      Object o = result.get(parentKey);
      if (Objects.nonNull(o)) {
        Properties pro = (Properties) o;
        pro.put(key, entry.getValue());
        result.put(parentKey, pro);
      } else {
        Properties pro1 = new Properties();
        pro1.put(key, entry.getValue());
        result.put(parentKey, pro1);
      }
    }
    return result;
  }

}

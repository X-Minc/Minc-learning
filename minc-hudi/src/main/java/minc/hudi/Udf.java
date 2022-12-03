package minc.hudi;

import org.apache.flink.table.functions.ScalarFunction;

import java.time.Duration;

/**
 * @Author: Minc
 * @DateTime: 2022/11/10
 */
public class Udf {
    public static class AliasFunction extends ScalarFunction {
        public String eval(String url) {
            url = url.replace("https://", "").replace("http://", "");
            url = url.substring(url.indexOf('/') + 1);
            return url
                    .replaceAll("/", ".")
                    .replaceAll("\\{", "")
                    .replaceAll("}", "");
        }
    }

    public static class GetTimeFunction extends ScalarFunction {
        public String eval(Long time) throws Exception {
            return DateTransformUtil.getStringDateAfterOffsetFromDate(time / 1000, "yyyy-MM-dd HH:mm:ss", Duration.ZERO);
        }
    }

    public static class GetTimePartitionFunction extends ScalarFunction {
        public Long eval(Long costMilliseconds) throws Exception {
            if (costMilliseconds < 1000L) {
                return 1000L;
            } else if (costMilliseconds < 3000L) {
                return 3000L;
            } else if (costMilliseconds < 5000L) {
                return 5000L;
            } else if (costMilliseconds < 30000L) {
                return 30000L;
            } else if (costMilliseconds < 300000L) {
                return 300000L;
            } else {
                return 0L;
            }
        }
    }
}

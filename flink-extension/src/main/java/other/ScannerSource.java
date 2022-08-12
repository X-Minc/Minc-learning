package other;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Scanner;

/**
 * @Author: Minc
 * @DateTime: 2022/7/18
 */
public class ScannerSource extends RichSourceFunction<Test.TestPojo> {
    @Override
    public void run(SourceContext<Test.TestPojo> sourceContext) throws Exception {
        Scanner scanner = new Scanner(System.in);
        String content;
        while ((content = scanner.nextLine()) != null) {
            String[] split = content.split(",", -1);
            Test.TestPojo testPojo = new Test.TestPojo();
            testPojo.setName(split[0]);
            testPojo.setTime(Long.parseLong(split[1]));
            sourceContext.collect(testPojo);
        }
    }

    @Override
    public void cancel() {

    }
}

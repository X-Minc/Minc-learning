package other;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.minc.operator.extension.TimerWindowProcess;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @Author: Minc
 * @DateTime: 2022/8/12
 */
public class Test {


    public static void main(String[] args) throws Exception {
        TimerWindowProcess<String, TestPojo, TestPojo> build = TimerWindowProcess
                .<String, TestPojo, TestPojo>builder()
                .setTrigger(true)
                .setTriggerInterval(1000L * 60)
                .setOffset(0L)
                .setWindowSize(1000L * 60 * 5)
                .setTimerWindowAdaptor(new TimerWindowProcess.TimerWindowAdaptor<String, TestPojo, TestPojo>() {
                    @Override
                    public void triggerProcess(String s, TimerWindowProcess.MidTriggerWindow timeWindow, List<TestPojo> rows, Collector<TestPojo> out) throws Exception {
                        TestPojo testPojo = new TestPojo();
                        testPojo.setName(s);
                        testPojo.setNum(rows.size());
                        testPojo.setStartTime(timeWindow.getStart());
                        testPojo.setEndTime(timeWindow.getEnd());
                        out.collect(testPojo);
                    }

                    @Override
                    public TypeInformation<TestPojo> getProducedType() {
                        return TypeInformation.of(TestPojo.class);
                    }
                })
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L * 60);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000L * 30));
        env.setParallelism(1);
        DataStreamSource<TestPojo> source = env.addSource(new ScannerSource());
        source.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TestPojo>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getTime()))
                .keyBy(TestPojo::getName)
                .process(build)
                .print();
        env.execute();
    }
}
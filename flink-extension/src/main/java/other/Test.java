package other;

import operator.extension.TimerWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @Author: Minc
 * @DateTime: 2022/8/12
 */
public class Test {
    public static class TestPojo {
        private String name;
        private Long time;
        private Integer num = 1;
        private Long startTime;
        private Long endTime;

        public Long getStartTime() {
            return startTime;
        }

        public void setStartTime(Long startTime) {
            this.startTime = startTime;
        }

        public Long getEndTime() {
            return endTime;
        }

        public void setEndTime(Long endTime) {
            this.endTime = endTime;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Long getTime() {
            return time;
        }

        public void setTime(Long time) {
            this.time = time;
        }

        public Integer getNum() {
            return num;
        }

        public void setNum(Integer num) {
            this.num = num;
        }

        @Override
        public String toString() {
            return "TestPojo{" +
                    "name='" + name + '\'' +
                    ", time=" + time +
                    ", num=" + num +
                    ", startTime=" + startTime +
                    ", endTime=" + endTime +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        TimerWindow<String, TestPojo, TestPojo> build = TimerWindow
                .<String, TestPojo, TestPojo>builder()
                .setMidTrigger(true)
                .setMidTriggerInterval(1000L * 60)
                .setOffset(0L)
                .setWindowSize(1000L * 60 * 5)
                .setTimerWindowAdaptor(new TimerWindow.TimerWindowAdaptor<String, TestPojo, TestPojo>() {
                    @Override
                    public void triggerProcess(String s, TimeWindow timeWindow, List<TestPojo> rows, Collector<TestPojo> out) throws Exception {
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
                .<TestPojo>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> event.getTime()))
                .keyBy(TestPojo::getName)
                .process(build)
                .print();
        env.execute();
    }
}

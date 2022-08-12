package operator.extension;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * @Author: Minc
 * @DateTime: 2022/7/29
 */
public class TtlKeyedProcessFunction<KEY, IN, OUT> extends KeyedProcessFunction<KEY, IN, OUT> {
    protected TtlKeyedProcessFunctionAdaptor<KEY, IN, OUT> ttlKeyedProcessFunctionAdaptor;
    protected Long ttlTime = 1000L * 60 * 30;
    protected ValueState<Long> time;
    protected ValueState<Boolean> drop;

    private static class TtlKeyedProcessFunctionBuilder<KEY, IN, OUT> {
        private final TtlKeyedProcessFunction<KEY, IN, OUT> ttlKeyedProcessFunction;

        private TtlKeyedProcessFunctionBuilder(TtlKeyedProcessFunction<KEY, IN, OUT> ttlKeyedProcessFunction) {
            this.ttlKeyedProcessFunction = ttlKeyedProcessFunction;
        }

        public TtlKeyedProcessFunctionBuilder<KEY, IN, OUT> setTime(ValueState<Long> time) {
            this.ttlKeyedProcessFunction.time = time;
            return this;
        }

        public TtlKeyedProcessFunctionBuilder<KEY, IN, OUT> setTtlKeyedProcessFunctionAdaptor(TtlKeyedProcessFunctionAdaptor<KEY, IN, OUT> ttlKeyedProcessFunctionAdaptor) {
            this.ttlKeyedProcessFunction.ttlKeyedProcessFunctionAdaptor = ttlKeyedProcessFunctionAdaptor;
            return this;
        }

        public TtlKeyedProcessFunction<KEY, IN, OUT> build() {
            return ttlKeyedProcessFunction;
        }
    }

    public interface TtlKeyedProcessFunctionAdaptor<KEY, IN, OUT> {
        void onTimerProcess(long timestamp, KeyedProcessFunction<KEY, IN, OUT>.OnTimerContext ctx, Collector<OUT> out) throws Exception;

        void openProcess(Configuration parameters) throws Exception;

        void process(IN value, KeyedProcessFunction<KEY, IN, OUT>.Context ctx, Collector<OUT> out) throws Exception;
    }

    public static <KEY, IN, OUT> TtlKeyedProcessFunctionBuilder<KEY, IN, OUT> builder() {
        return new TtlKeyedProcessFunctionBuilder<>(new TtlKeyedProcessFunction<KEY, IN, OUT>());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
        time.clear();
        drop.update(true);
        ttlKeyedProcessFunctionAdaptor.onTimerProcess(timestamp, ctx, out);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> timeStateDescriptor = new ValueStateDescriptor<>("time", BasicTypeInfo.LONG_TYPE_INFO);
        time = getRuntimeContext().getState(timeStateDescriptor);
        ValueStateDescriptor<Boolean> dropStateDescriptor = new ValueStateDescriptor<>("drop", BasicTypeInfo.BOOLEAN_TYPE_INFO);
        drop = getRuntimeContext().getState(dropStateDescriptor);
        ttlKeyedProcessFunctionAdaptor.openProcess(parameters);
    }

    @Override
    public void processElement(IN value, Context ctx, Collector<OUT> out) throws Exception {
        if (Objects.isNull(drop.value())) {
            if (Objects.nonNull(time.value())) {
                ctx.timerService().deleteEventTimeTimer(time.value());
            }
            long next = ctx.timestamp() + ttlTime;
            time.update(next);
            ctx.timerService().registerEventTimeTimer(next);
            ttlKeyedProcessFunctionAdaptor.process(value, ctx, out);
        }
    }
}

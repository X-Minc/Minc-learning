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
public abstract class TtlKeyedProcessFunction<KEY, IN, OUT> extends KeyedProcessFunction<KEY, IN, OUT> {
    protected Long ttlTime = 1000L * 60 * 30;
    protected ValueState<Long> time;
    protected ValueState<Boolean> drop;

    public TtlKeyedProcessFunction(Long ttlTime) {
        this.ttlTime = ttlTime;
    }

    public TtlKeyedProcessFunction() {
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
        time.clear();
        drop.update(true);
        onTimerProcess(timestamp, ctx, out);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> timeStateDescriptor = new ValueStateDescriptor<>("time", BasicTypeInfo.LONG_TYPE_INFO);
        time = getRuntimeContext().getState(timeStateDescriptor);
        ValueStateDescriptor<Boolean> dropStateDescriptor = new ValueStateDescriptor<>("drop", BasicTypeInfo.BOOLEAN_TYPE_INFO);
        drop = getRuntimeContext().getState(dropStateDescriptor);
        openProcess(parameters);
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
            process(value, ctx, out);
        }
    }

    protected abstract void onTimerProcess(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception;

    protected abstract void openProcess(Configuration parameters) throws Exception;

    protected abstract void process(IN value, Context ctx, Collector<OUT> out) throws Exception;
}

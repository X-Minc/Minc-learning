package operator.extension;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.*;

/**
 * @Author: Minc
 * @DateTime: 2022/7/28
 */
public class TimerWindow<KEY, IN, OUT> extends KeyedProcessFunction<KEY, IN, OUT> implements ResultTypeQueryable<OUT> {
    private MapState<TimeWindow, List<IN>> rows;
    private MapState<Long, TimeWindow> mid;
    private TimerWindowAdaptor<KEY, IN, OUT> timerWindowAdaptor;
    private ValueState<TimeWindow> drop;
    private Long windowSize;
    private Boolean midTrigger;
    private Long midTriggerInterval;
    private Long offset;

    public static class TimerWindowBuilder<KEY, IN, OUT> {
        private final TimerWindow<KEY, IN, OUT> timerWindow;

        private TimerWindowBuilder(TimerWindow<KEY, IN, OUT> timerWindow) {
            this.timerWindow = timerWindow;
        }

        public TimerWindowBuilder<KEY, IN, OUT> setWindowSize(Long windowSize) {
            this.timerWindow.windowSize = windowSize;
            return this;

        }

        public TimerWindowBuilder<KEY, IN, OUT> setMidTrigger(Boolean midTrigger) {
            this.timerWindow.midTrigger = midTrigger;
            return this;

        }

        public TimerWindowBuilder<KEY, IN, OUT> setMidTriggerInterval(Long midTriggerInterval) {
            this.timerWindow.midTriggerInterval = midTriggerInterval;
            return this;

        }

        public TimerWindowBuilder<KEY, IN, OUT> setTimerWindowAdaptor(TimerWindowAdaptor<KEY, IN, OUT> timerWindowAdaptor) {
            this.timerWindow.timerWindowAdaptor = timerWindowAdaptor;
            return this;
        }

        public TimerWindowBuilder<KEY, IN, OUT> setOffset(Long offset) {
            this.timerWindow.offset = offset;
            return this;
        }

        public TimerWindow<KEY, IN, OUT> build() {
            return timerWindow;
        }

    }

    public interface TimerWindowAdaptor<KEY, IN, OUT> extends Serializable, ResultTypeQueryable<OUT> {
        void triggerProcess(KEY key, TimeWindow timeWindow, List<IN> rows, Collector<OUT> out) throws Exception;
    }


    public static <KEY, IN, OUT> TimerWindowBuilder<KEY, IN, OUT> builder() {
        return new TimerWindowBuilder<>(new TimerWindow<>());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
        Long now = ctx.timestamp();
        TimeWindow timeWindow;
        boolean finalStep = false;
        if (this.midTrigger) {
            timeWindow = this.mid.get(now);
            this.mid.remove(now);
            long nextTrigger = now + this.midTriggerInterval;
            if (!Objects.equals(now, timeWindow.maxTimestamp())) {
                if (nextTrigger <= timeWindow.maxTimestamp()) {
                    ctx.timerService().registerEventTimeTimer(nextTrigger);
                    this.mid.put(nextTrigger, timeWindow);
                } else {
                    ctx.timerService().registerEventTimeTimer(timeWindow.maxTimestamp());
                }
            } else {
                finalStep = true;
            }
        } else {
            timeWindow = getWindow(now, 0L, this.windowSize);
            finalStep = true;
        }
        List<IN> rows = this.rows.get(timeWindow);
        this.timerWindowAdaptor.triggerProcess(ctx.getCurrentKey(), timeWindow, rows, out);
        if (finalStep) {
            this.rows.remove(timeWindow);
            this.drop.update(timeWindow);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Preconditions.checkArgument(midTrigger && windowSize / midTriggerInterval != 0, "midTriggerInterval must be a multiple of windowSize !");
        MapStateDescriptor<TimeWindow, List<IN>> rowsMapState = new MapStateDescriptor<>("rows", TypeInformation.of(TimeWindow.class), TypeInformation.of(new TypeHint<List<IN>>() {
        }));
        this.rows = getRuntimeContext().getMapState(rowsMapState);
        ValueStateDescriptor<TimeWindow> dropMapState = new ValueStateDescriptor<>("drop", TypeInformation.of(TimeWindow.class));
        this.drop = getRuntimeContext().getState(dropMapState);
        MapStateDescriptor<Long, TimeWindow> midMapState = new MapStateDescriptor<>("mid", BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(TimeWindow.class));
        this.mid = getRuntimeContext().getMapState(midMapState);
    }

    @Override
    public void processElement(IN value, Context ctx, Collector<OUT> out) throws Exception {
        Long now = ctx.timestamp();
        TimeWindow window = getWindow(now, this.offset, this.windowSize);
        if (Objects.isNull(drop.value())) {
            drop.update(window);
        }
        if (window.maxTimestamp() >= drop.value().maxTimestamp()) {
            List<IN> values = this.rows.get(window);
            if (Objects.isNull(values)) {
                values = new ArrayList<>();
                if (this.midTrigger) {
                    long triggerTime = getTriggerTime(now, this.midTriggerInterval) + this.midTriggerInterval;
                    ctx.timerService().registerEventTimeTimer(triggerTime);
                    this.mid.put(triggerTime, window);
                } else {
                    ctx.timerService().registerEventTimeTimer(window.maxTimestamp());
                }
            }
            values.add(value);
            this.rows.put(window, values);
        }
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return timerWindowAdaptor.getProducedType();
    }

    private TimeWindow getWindow(long now, long offset, long winSize) throws Exception {
        long startWithOffset = TimeWindow.getWindowStartWithOffset(now, offset, winSize);
        return new TimeWindow(startWithOffset, startWithOffset + winSize);
    }

    private Long getTriggerTime(Long now, Long interval) throws Exception {
        return now / interval * interval - 1;
    }
}

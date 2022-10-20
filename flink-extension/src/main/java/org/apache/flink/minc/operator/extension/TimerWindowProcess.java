package org.apache.flink.minc.operator.extension;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
public class TimerWindowProcess<KEY, IN, OUT> extends KeyedProcessFunction<KEY, IN, OUT> implements ResultTypeQueryable<OUT> {
    private MapState<MidTriggerWindow, List<IN>> cache;
    private TimerWindowAdaptor<KEY, IN, OUT> timerWindowAdaptor;
    private ValueState<Long> timeLine;
    private Long windowSize;
    private Long offset = 0L;
    private Boolean trigger = false;
    private Long triggerInterval;

    public interface TimerWindowAdaptor<KEY, IN, OUT> extends Serializable, ResultTypeQueryable<OUT> {
        void triggerProcess(KEY key, MidTriggerWindow timeWindow, List<IN> rows, Collector<OUT> out) throws Exception;
    }

    public static <KEY, IN, OUT> TimerWindowBuilder<KEY, IN, OUT> builder() {
        return new TimerWindowBuilder<>(new TimerWindowProcess<>());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
        Long now = ctx.timestamp();
        MidTriggerWindow window = getWindow(now);
        List<IN> lines = this.cache.get(window);
        this.timerWindowAdaptor.triggerProcess(ctx.getCurrentKey(), window, lines, out);
        Long nextTriggerTime = window.getNextTriggerTime(now);
        if (Objects.nonNull(nextTriggerTime)) {
            ctx.timerService().registerEventTimeTimer(window.getNextTriggerTime(now));
        } else {
            this.cache.remove(window);
            this.timeLine.update(window.getEnd());
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Preconditions.checkArgument(Objects.nonNull(windowSize), "windowSize must not be null !");
        if (trigger) {
            Preconditions.checkArgument(Objects.nonNull(triggerInterval), "midTriggerInterval must not be null when midTrigger is true !");
        }
        MapStateDescriptor<MidTriggerWindow, List<IN>> rowsMapState = new MapStateDescriptor<>("cache", TypeInformation.of(MidTriggerWindow.class), TypeInformation.of(new TypeHint<List<IN>>() {
        }));
        this.cache = getRuntimeContext().getMapState(rowsMapState);
        ValueStateDescriptor<Long> timeLineMapState = new ValueStateDescriptor<>("tineLine", BasicTypeInfo.LONG_TYPE_INFO);
        this.timeLine = getRuntimeContext().getState(timeLineMapState);
    }

    @Override
    public void processElement(IN value, Context ctx, Collector<OUT> out) throws Exception {
        Long now = ctx.timestamp();
        //获得当前事件时间所属于的窗口
        MidTriggerWindow window = getWindow(now);
        //初始化时间线
        if (Objects.isNull(timeLine.value())) {
            timeLine.update(window.getStart());
        }
        //过滤小于初始时间线的数据
        if (now >= timeLine.value()) {
            List<IN> values = this.cache.get(window);
            if (Objects.isNull(values)) {
                values = new ArrayList<>();
                //根据是否中途触发编列不同逻辑
                if (this.trigger) {
                    //获得下次触发时间
                    Long nextTriggerTime = window.getNextTriggerTime(now);
                    if (Objects.nonNull(nextTriggerTime)) {
                        ctx.timerService().registerEventTimeTimer(nextTriggerTime);
                    }
                } else {
                    ctx.timerService().registerEventTimeTimer(window.maxTimestamp());
                }
            }
            values.add(value);
            this.cache.put(window, values);
        }
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return timerWindowAdaptor.getProducedType();
    }

    private MidTriggerWindow getWindow(long now) throws Exception {
        long startWithOffset = MidTriggerWindow.getWindowStartWithOffset(now, offset, this.windowSize);
        if (trigger) {
            return new MidTriggerWindow(startWithOffset, startWithOffset + this.windowSize, triggerInterval);
        } else {
            return new MidTriggerWindow(startWithOffset, startWithOffset + this.windowSize);
        }
    }

    /**
     * MidTriggerWindow
     */
    public static class MidTriggerWindow extends TimeWindow {
        protected Long triggerFrequency;

        public MidTriggerWindow(long start, long end) {
            super(start, end);
        }

        public MidTriggerWindow(long start, long end, Long triggerFrequency) {
            super(start, end);
            this.triggerFrequency = triggerFrequency;
        }

        public Long getTriggerFrequency() {
            return triggerFrequency;
        }

        public void setTriggerFrequency(Long triggerFrequency) {
            this.triggerFrequency = triggerFrequency;
        }

        public Long getNextTriggerTime(Long now) {
            if (now < super.maxTimestamp()) {
                long next = now + triggerFrequency - 1;
                return Math.min(next, super.maxTimestamp());
            } else {
                return null;
            }
        }

        @Override
        public String toString() {
            return super.toString();
        }
    }

    /**
     * builder
     */
    public static class TimerWindowBuilder<KEY, IN, OUT> {
        private final TimerWindowProcess<KEY, IN, OUT> timerWindowProcess;

        private TimerWindowBuilder(TimerWindowProcess<KEY, IN, OUT> timerWindowProcess) {
            this.timerWindowProcess = timerWindowProcess;
        }

        public TimerWindowBuilder<KEY, IN, OUT> setWindowSize(Long windowSize) {
            this.timerWindowProcess.windowSize = windowSize;
            return this;

        }

        public TimerWindowBuilder<KEY, IN, OUT> setTrigger(Boolean midTrigger) {
            this.timerWindowProcess.trigger = midTrigger;
            return this;

        }

        public TimerWindowBuilder<KEY, IN, OUT> setTriggerInterval(Long midTriggerInterval) {
            this.timerWindowProcess.triggerInterval = midTriggerInterval;
            return this;

        }

        public TimerWindowBuilder<KEY, IN, OUT> setTimerWindowAdaptor(TimerWindowAdaptor<KEY, IN, OUT> timerWindowAdaptor) {
            this.timerWindowProcess.timerWindowAdaptor = timerWindowAdaptor;
            return this;
        }

        public TimerWindowBuilder<KEY, IN, OUT> setOffset(Long offset) {
            this.timerWindowProcess.offset = offset;
            return this;
        }

        public TimerWindowProcess<KEY, IN, OUT> build() {
            return timerWindowProcess;
        }

    }
}
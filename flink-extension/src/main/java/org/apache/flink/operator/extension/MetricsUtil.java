package org.apache.flink.operator.extension;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;

/**
 * Metrics Util Class,
 * UpStream's out flow num is downStream's in flow num,so don't exist in flow num.
 *
 * @author Minc
 * @date 2021-10-27 15:51
 */
public class MetricsUtil {
    public enum MetricsGroup {
        sink("sink"),
        midProcess("midProcess"),
        source("source");
        private final String name;

        MetricsGroup(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private static final String OUT_TRANSACTIONS_PER_SECOND = "outTPS";
    private static final String IN_TRANSACTIONS_PER_SECOND = "inTPS";
    private static final String OUT_BIT_PER_SECOND = "outBPS";
    private static final String IN_BIT_PER_SECOND = "inBPS";
    private static final String TRIGGERED_PER_WINDOW_SIZE = "triggeredPWS";
    private static final Integer DEFAULT_TIME_SPAN_IN_SECONDS = 1;


    /**
     * 注册一个监控触发窗口size的meter
     *
     * @param context 上下文对象
     * @return meter对象
     */
    public static Meter registerTriggeredPWSMeter(MetricsGroup metricsGroup, RuntimeContext context) {
        return context.getMetricGroup().addGroup(metricsGroup.getName()).meter(TRIGGERED_PER_WINDOW_SIZE, new MeterView(new SimpleCounter(), DEFAULT_TIME_SPAN_IN_SECONDS));
    }

    /**
     * 注册一个监控每秒输出bit流量的meter
     *
     * @param context 上下文对象
     * @return meter对象
     */
    public static Meter registerOutBPSMeter(MetricsGroup metricsGroup, RuntimeContext context) {
        return context.getMetricGroup().addGroup(metricsGroup.getName()).meter(OUT_BIT_PER_SECOND, new MeterView(new SimpleCounter(), DEFAULT_TIME_SPAN_IN_SECONDS));
    }

    /**
     * 注册一个监控每秒数据输出流量的meter
     *
     * @param context 上下文对象
     * @return meter对象
     */
    public static Meter registerOutTPSMeter(MetricsGroup metricsGroup, RuntimeContext context) {
        return context.getMetricGroup().addGroup(metricsGroup.getName()).meter(OUT_TRANSACTIONS_PER_SECOND, new MeterView(new SimpleCounter(), DEFAULT_TIME_SPAN_IN_SECONDS));
    }

    /**
     * 注册一个监控每秒输入bit流量的meter
     *
     * @param context 上下文对象
     * @return meter对象
     */
    public static Meter registerInBPSMeter(MetricsGroup metricsGroup, RuntimeContext context) {
        return context.getMetricGroup().addGroup(metricsGroup.getName()).meter(IN_BIT_PER_SECOND, new MeterView(new SimpleCounter(), DEFAULT_TIME_SPAN_IN_SECONDS));
    }

    /**
     * 注册一个监控每秒数据输入流量的meter
     *
     * @param context 上下文对象
     * @return meter对象
     */
    public static Meter registerInTPSMeter(MetricsGroup metricsGroup, RuntimeContext context) {
        return context.getMetricGroup().addGroup(metricsGroup.getName()).meter(IN_TRANSACTIONS_PER_SECOND, new MeterView(new SimpleCounter(), DEFAULT_TIME_SPAN_IN_SECONDS));
    }
}

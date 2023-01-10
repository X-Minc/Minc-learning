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
        sink,
        midOperator,
        source
    }

    public enum OperateMetricsType {
        ROW_OUTFLOW_PER_SECOND,
        ROW_INFLOW_PER_SECOND,
        BIT_OUTFLOW_PER_SECOND,
        BIT_INFLOW_PER_SECOND
    }

    private static final Integer DEFAULT_TIME_SPAN_IN_SECONDS = 1;

    /**
     * 注册一个监控每秒数据输入流量的meter
     *
     * @param metricsGroup 算子类型
     * @param context 上下文对象
     * @param operateMetricsType 算子监控类型
     * @return meter对象
     */
    public static Meter registerInflowMeter(MetricsGroup metricsGroup, OperateMetricsType operateMetricsType, RuntimeContext context) {
        return context
                .getMetricGroup()
                .addGroup(metricsGroup.name())
                .meter(operateMetricsType.name(), new MeterView(new SimpleCounter(), DEFAULT_TIME_SPAN_IN_SECONDS));
    }
}

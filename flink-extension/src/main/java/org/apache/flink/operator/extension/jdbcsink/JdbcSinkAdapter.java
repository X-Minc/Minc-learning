package org.apache.flink.operator.extension.jdbcsink;

import java.io.Serializable;
import java.sql.Connection;
import java.util.Map;

/**
 * @Author: Minc
 * @DateTime: 2022.6.14 0014
 */
public interface JdbcSinkAdapter<T> extends Serializable {
    // T transform to your data
    Object[] getValues(T t) throws Exception;
}

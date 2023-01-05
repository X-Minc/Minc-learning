package org.apache.flink.operator.extension.hbasesink;

import org.apache.hadoop.hbase.client.Put;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

/**
 * @Author: Minc
 * @DateTime: 2022/7/5
 */
public abstract class DataTransform<IN> implements Serializable {

    protected String family;

    protected DataTransform(String family) {
        this.family = family;
    }

    protected abstract Put doTransform(IN in, Properties hbaseProperties) throws Exception;
}

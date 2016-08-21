package com.jd.common.hbase.client.row;

import com.jd.common.hbase.exception.HBaseRowMappingException;
import org.apache.hadoop.hbase.client.Result;

public abstract interface RowMapper<T> {
    public abstract T mapRow(Result paramResult)
            throws HBaseRowMappingException;
}

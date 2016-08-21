package com.jd.hbase.leman.client.row;

import com.jd.hbase.leman.exception.HBaseRowMappingException;
import org.apache.hadoop.hbase.client.Result;

public abstract interface RowMapper<T> {
    public abstract T mapRow(Result paramResult)
            throws HBaseRowMappingException;
}

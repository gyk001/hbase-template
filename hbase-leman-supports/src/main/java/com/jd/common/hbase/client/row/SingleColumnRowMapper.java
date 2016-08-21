package com.jd.common.hbase.client.row;

import com.jd.common.hbase.client.util.BytesUtil;
import com.jd.common.hbase.exception.HBaseRowMappingException;
import org.apache.hadoop.hbase.client.Result;

public class SingleColumnRowMapper<T>
        implements RowMapper<T> {
    private Class<T> requiredType;
    private String family;
    private String qualifier;

    public SingleColumnRowMapper(String family, String qualifier, Class<T> requiredType) {
        this.family = family;
        this.qualifier = qualifier;
        this.requiredType = requiredType;
    }

    public T mapRow(Result rs)
            throws HBaseRowMappingException {
        Object result = getColumnValue(rs, this.family, this.qualifier, this.requiredType);
        return (T) result;
    }

    protected Object getColumnValue(Result rs, String family, String qualifier, Class<? extends Object> requiredType)
            throws HBaseRowMappingException {
        if (requiredType != null) {
            return getResultValue(rs, family, qualifier, requiredType);
        }
        return getResultValue(rs, family, qualifier);
    }

    private Object getResultValue(Result rs, String family, String qualifier, Class<? extends Object> requiredType)
            throws HBaseRowMappingException {
        Object obj = null;
        try {
            byte[] b = (byte[]) getResultValue(rs, family, qualifier);
            obj = BytesUtil.toObject(b, requiredType);
        } catch (Exception e) {
            throw new HBaseRowMappingException("", e);
        }
        return obj;
    }

    public Object getResultValue(Result rs, String family, String qualifier)
            throws HBaseRowMappingException {
        Object obj = null;
        try {
            obj = rs.getValue(BytesUtil.stringToBytes(family), BytesUtil.stringToBytes(qualifier));
        } catch (Exception e) {
            throw new HBaseRowMappingException("", e);
        }
        return obj;
    }

    public Class<T> getRequiredType() {
        return this.requiredType;
    }
}

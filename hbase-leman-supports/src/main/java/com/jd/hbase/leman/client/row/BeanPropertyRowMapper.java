package com.jd.hbase.leman.client.row;

import com.jd.hbase.leman.client.util.BytesUtil;
import com.jd.hbase.leman.client.util.ReflectUtil;
import com.jd.hbase.leman.exception.HBaseRowMappingException;
import com.jd.hbase.leman.annotation.Column;
import com.jd.hbase.leman.annotation.RowKey;

import org.apache.hadoop.hbase.client.Result;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class BeanPropertyRowMapper<T>
        implements RowMapper<T> {
    private Class<T> requiredType;

    public BeanPropertyRowMapper(Class<T> requiredType) {
        this.requiredType = requiredType;
    }

    public T mapRow(Result result)
            throws HBaseRowMappingException {
        if ((result == null) || (result.isEmpty())) {
            return null;
        }
        try {
            T value = this.requiredType.newInstance();

            Field[] fields = this.requiredType.getDeclaredFields();
            for (Field field : fields) {
                Method writeMethod = ReflectUtil.getWriteMethod(this.requiredType, field);
                if (writeMethod != null) {
                    Class<?>[] clazzs = writeMethod.getParameterTypes();
                    if ((clazzs != null) && (clazzs.length == 1)) {
                        if (field.isAnnotationPresent(RowKey.class)) {
                            writeMethod.invoke(value, new Object[]{BytesUtil.toObject(result.getRow(), field.getType())});
                        } else if (field.isAnnotationPresent(Column.class)) {
                            Column column = ReflectUtil.getColumn(field);
                            byte[] fieldValue = result.getValue(BytesUtil.toBytes(column.family()), BytesUtil.toBytes(column.name()));
                            if ((fieldValue != null) && (fieldValue.length > 0)) {
                                Object obj = BytesUtil.toObject(fieldValue, field.getType());
                                if (obj != null) {
                                    writeMethod.invoke(value, new Object[]{obj});
                                }
                            }
                        }
                    }
                }
            }
            return value;
        } catch (Exception e) {
            throw new HBaseRowMappingException("error occurs when mapping row!", e);
        }
    }
}

package com.jd.common.hbase.client.util;



import com.jd.common.hbase.client.entity.HBaseColumn;
import com.jd.common.hbase.client.entity.HBaseDataStructure;
import com.jd.hbase.leman.annotation.Column;
import com.jd.hbase.leman.annotation.RowKey;
import com.jd.hbase.leman.annotation.Table;

import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ReflectUtil {
    public static String getTableName(Object obj) {
        return getTableName(obj.getClass());
    }

    public static Field getField(Class<?> clazz, String fieldName) {
        if (clazz == null) {
            throw new IllegalArgumentException("clazz is null");
        }
        if (fieldName == null) {
            throw new IllegalArgumentException("fieldName is null");
        }
        try {
            return clazz.getDeclaredField(fieldName);
        } catch (SecurityException e) {
            throw new IllegalArgumentException("can not parse the field:" + fieldName, e);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("field " + fieldName + " is not exists", e);
        }
    }

    public static String getTableName(Class<?> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("clazz is null");
        }
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("@Table is not been set!");
        }
        Table table = (Table) clazz.getAnnotation(Table.class);
        return table.value();
    }

    public static String getFamily(Class<?> clazz, String fieldName) {
        return getColumn(clazz, fieldName).family();
    }

    public static String getName(Class<?> clazz, String fieldName) {
        return getColumn(clazz, fieldName).name();
    }

    public static Column getColumn(Class<?> clazz, String fieldName) {
        return getColumn(getField(clazz, fieldName));
    }

    public static Column getColumn(Field field) {
        if (field == null) {
            throw new IllegalArgumentException("field is null");
        }
        if (!field.isAnnotationPresent(Column.class)) {
            throw new IllegalArgumentException("@Column is not found!");
        }
        return (Column) field.getAnnotation(Column.class);
    }

    public static List<HBaseColumn> getHBaseColumnList(Class<?> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("clazz is null");
        }
        List<HBaseColumn> columnList = new ArrayList();
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(Column.class)) {
                columnList.add(getHBaseColumn(field));
            }
        }
        return columnList;
    }

    public static List<HBaseColumn> getHBaseColumnList(Object obj) {
        Class<? extends Object> clazz = obj.getClass();
        List<HBaseColumn> columnList = new ArrayList();
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(Column.class)) {
                HBaseColumn hbaseColumn = getHBaseColumn(field, obj);
                columnList.add(hbaseColumn);
            }
        }
        return columnList;
    }

    public static HBaseColumn getHBaseColumn(Class<?> clazz, String fieldName) {
        return getHBaseColumn(getField(clazz, fieldName));
    }

    public static HBaseColumn getHBaseColumn(Field field) {
        Column column = getColumn(field);
        HBaseColumn hbaseColumn = new HBaseColumn();
        hbaseColumn.setFamily(BytesUtil.stringToBytes(column.family()));
        hbaseColumn.setQualifier(BytesUtil.stringToBytes(column.name()));
        return hbaseColumn;
    }

    public static HBaseColumn getHBaseColumn(Object obj, String fieldName) {
        if (obj == null) {
            throw new IllegalArgumentException("obj is null");
        }
        if (fieldName == null) {
            throw new IllegalArgumentException("fieldName is null");
        }
        return getHBaseColumn(getField(obj.getClass(), fieldName), obj);
    }

    public static HBaseColumn getHBaseColumn(Field field, Object obj) {
        HBaseColumn hbaseColumn = getHBaseColumn(field);
        Object fieldValue = getFieldValue(obj, field.getName());

        hbaseColumn.setValue(BytesUtil.toBytes(fieldValue));
        return hbaseColumn;
    }

    public static Method getWriteMethod(Class<?> clazz, Field field) {
        if ((clazz == null) || (field == null)) {
            return null;
        }
        String name = field.getName();
        String method = "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
        try {
            return clazz.getDeclaredMethod(method, new Class[]{field.getType()});
        } catch (SecurityException e) {
        } catch (NoSuchMethodException e) {
        }
        return null;
    }

    public static Method getReadMethod(Class<?> clazz, String fieldName) {
        if ((clazz == null) || (fieldName == null)) {
            return null;
        }
        String method = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        try {
            return clazz.getDeclaredMethod(method, new Class[0]);
        } catch (SecurityException e) {
        } catch (NoSuchMethodException e) {
        }
        return null;
    }

    public static Object getFieldValue(Object obj, String fieldName) {
        if (obj == null) {
            return null;
        }
        if (fieldName == null) {
            throw new IllegalArgumentException("fieldName is null");
        }
        Method readMethod = getReadMethod(obj.getClass(), fieldName);
        if (readMethod != null) {
            try {
                return readMethod.invoke(obj, new Object[0]);
            } catch (IllegalArgumentException e) {
            } catch (IllegalAccessException e) {
            } catch (InvocationTargetException e) {
            }
        }
        return null;
    }

    public static Field getRowKey(Class<?> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("clazz is null");
        }
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(RowKey.class)) {
                return field;
            }
        }
        return null;
    }

    public static String getRowKey(Object obj) {
        if (obj == null) {
            return null;
        }
        Field field = getRowKey(obj.getClass());
        if (field == null) {
            return null;
        }
        Object value = getFieldValue(obj, field.getName());
        if (value != null) {
            return Bytes.toString(BytesUtil.toBytes(value));
        }
        return null;
    }

    public static HBaseDataStructure getHBaseDataStructure(Object obj) {
        Class<? extends Object> clazz = obj.getClass();
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("@Table is not set!");
        }
        HBaseDataStructure struct = new HBaseDataStructure();
        Table table = (Table) clazz.getAnnotation(Table.class);
        struct.setTableName(table.value());

        List<HBaseColumn> columnList = new ArrayList();
        struct.setColumns(columnList);

        HBaseColumn hBaseColumn = null;
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(RowKey.class)) {
                struct.setRowKey(Bytes.toString(BytesUtil.toBytes(getFieldValue(obj, field.getName()))));
            } else if (field.isAnnotationPresent(Column.class)) {
                hBaseColumn = getHBaseColumn(field, obj);
                columnList.add(hBaseColumn);
            }
        }
        return struct;
    }
}

package com.jd.hbase.leman.client.builder;

import com.jd.hbase.leman.client.entity.HBaseColumn;
import com.jd.hbase.leman.client.entity.HBaseDataStructure;
import com.jd.hbase.leman.client.util.BytesUtil;
import com.jd.hbase.leman.client.util.ReflectUtil;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

public class PutBuilder {
    public static List<Put> build(Object obj) {
        return build(obj, false);
    }

    public static List<Put> build(Object obj, boolean createMultiPut) {
        HBaseDataStructure struct = ReflectUtil.getHBaseDataStructure(obj);
        return build(struct.getRowKey(), struct.getColumns(), createMultiPut);
    }

    public static List<Put> buildBatchPuts(List<? extends Object> objs) {
        return buildBatchPuts(objs, false);
    }

    public static List<Put> buildBatchPuts(List<? extends Object> objs, boolean createMultiPut) {
        if (objs != null) {
            List<Put> puts = new ArrayList();
            for (Object obj : objs) {
                HBaseDataStructure struct = ReflectUtil.getHBaseDataStructure(obj);
                Put put = buildSinglePut(struct.getRowKey(), struct.getColumns());
                puts.add(put);
            }
            return puts;
        }
        return null;
    }

    public static List<Put> build(String rowKey, List<HBaseColumn> columns) {
        return build(rowKey, columns, false);
    }

    public static List<Put> build(String rowKey, List<HBaseColumn> columns, boolean createMultiPut) {
        List<Put> puts = new ArrayList();
        if (!createMultiPut) {
            puts.add(buildSinglePut(rowKey, columns));
        } else {
            Put put = new Put(BytesUtil.stringToBytes(rowKey));
            for (HBaseColumn column : columns) {
                put.add(column.getFamily(), column.getQualifier(), column.getValue());
            }
            puts.add(put);
        }
        return puts;
    }

    private static Put buildSinglePut(String rowKey, List<HBaseColumn> columns) {
        Put put = new Put(BytesUtil.stringToBytes(rowKey));
        for (HBaseColumn column : columns) {
            put.add(column.getFamily(), column.getQualifier(), column.getValue());
        }
        return put;
    }

    public static Put build(String rowKey, String family, String qualifier, Object value) {
        byte[] rowKeyBytes = BytesUtil.stringToBytes(rowKey);
        Put put = new Put(rowKeyBytes);
        put.add(BytesUtil.stringToBytes(family), BytesUtil.stringToBytes(qualifier), BytesUtil.toBytes(value));

        return put;
    }
}

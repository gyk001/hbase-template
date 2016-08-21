package com.jd.hbase.leman.client.builder;

import com.jd.hbase.leman.client.entity.HBaseColumn;
import com.jd.hbase.leman.client.entity.HBaseDataStructure;
import com.jd.hbase.leman.client.util.BytesUtil;
import com.jd.hbase.leman.client.util.ReflectUtil;
import com.jd.hbase.leman.annotation.Column;

import org.apache.hadoop.hbase.client.Get;

import java.util.ArrayList;
import java.util.List;

public class GetBuilder {
    public static Get build(String rowKey) {
        return new Get(BytesUtil.stringToBytes(rowKey));
    }

    public static List<Get> build(Object obj) {
        return build(obj, false);
    }

    public static List<Get> build(Object obj, boolean createMulti) {
        HBaseDataStructure struct = ReflectUtil.getHBaseDataStructure(obj);
        return build(struct.getRowKey(), struct.getColumns(), createMulti);
    }

    public static List<Get> build(String rowKey, List<HBaseColumn> columns, boolean createMulti) {
        List<Get> gets = new ArrayList();
        if (!createMulti) {
            Get get = build(rowKey);
            for (HBaseColumn col : columns) {
                get.addColumn(col.getFamily(), col.getQualifier());
            }
            gets.add(get);
        } else {
            for (HBaseColumn col : columns) {
                Get get = build(rowKey);
                get.addColumn(col.getFamily(), col.getQualifier());
                gets.add(get);
            }
        }
        return gets;
    }

    public static Get build(String rowKey, List<HBaseColumn> columns) {
        List<Get> gets = build(rowKey, columns, false);
        return (Get) gets.get(0);
    }

    public static Get build(String rowKey, String family, String qualifier) {
        Get get = build(rowKey);
        get.addColumn(BytesUtil.stringToBytes(family), BytesUtil.stringToBytes(qualifier));

        return get;
    }

    public static Get build(Object obj, String fieldName) {
        if (obj == null) {
            throw new IllegalArgumentException("obj is null");
        }
        if ((fieldName == null) || (fieldName.isEmpty())) {
            throw new IllegalArgumentException("fieldName is empty");
        }
        Column column = ReflectUtil.getColumn(obj.getClass(), fieldName);
        return build(ReflectUtil.getRowKey(obj), column.family(), column.name());
    }

    public static Get build(String rowKey, String... families) {
        Get get = build(rowKey);
        for (String family : families) {
            get.addFamily(BytesUtil.stringToBytes(family));
        }
        return get;
    }
}

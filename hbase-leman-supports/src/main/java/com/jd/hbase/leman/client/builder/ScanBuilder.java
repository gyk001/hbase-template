package com.jd.hbase.leman.client.builder;

import com.jd.hbase.leman.client.entity.HBaseColumn;
import com.jd.hbase.leman.client.util.BytesUtil;
import com.jd.hbase.leman.client.util.ReflectUtil;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;

public class ScanBuilder {
    public static Scan build(Object obj) {
        List<HBaseColumn> columns = ReflectUtil.getHBaseColumnList(obj);
        return build(columns);
    }

    public static Scan build(Class<?> clazz) {
        List<HBaseColumn> columns = ReflectUtil.getHBaseColumnList(clazz);
        return build(columns);
    }

    public static Scan build(List<HBaseColumn> columnList) {
        Scan scan = getScan();
        for (HBaseColumn column : columnList) {
            scan.addColumn(column.getFamily(), column.getQualifier());
        }
        return scan;
    }

    public static Scan buildStartRow(byte[] startRow) {
        Scan scan = getScan();
        scan.setStartRow(startRow);
        return scan;
    }

    public static Scan buildStopRow(byte[] stopRow) {
        Scan scan = getScan();
        scan.setStopRow(stopRow);
        return scan;
    }

    public static Scan build(String... families) {
        Scan scan = getScan();
        for (String family : families) {
            scan.addFamily(BytesUtil.stringToBytes(family));
        }
        return scan;
    }

    public static Scan getScan() {
        return new Scan();
    }
}

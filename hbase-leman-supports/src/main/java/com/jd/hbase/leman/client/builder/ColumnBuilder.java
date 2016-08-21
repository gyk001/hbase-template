package com.jd.hbase.leman.client.builder;

import com.jd.hbase.leman.client.entity.HBaseColumn;
import com.jd.hbase.leman.client.util.ReflectUtil;

import java.util.ArrayList;
import java.util.List;

public class ColumnBuilder {
    private Class<? extends Object> elementType;
    private List<HBaseColumn> columnList = new ArrayList();

    private ColumnBuilder(Class<? extends Object> elementType) {
        this.elementType = elementType;
    }

    public static ColumnBuilder newInstance(Class<? extends Object> elementType) {
        return new ColumnBuilder(elementType);
    }

    public ColumnBuilder addColumn(String fieldName) {
        this.columnList.add(ReflectUtil.getHBaseColumn(this.elementType, fieldName));

        return this;
    }

    public List<HBaseColumn> getColumnList() {
        return this.columnList;
    }
}

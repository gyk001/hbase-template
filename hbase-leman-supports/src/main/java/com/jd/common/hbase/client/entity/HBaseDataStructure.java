package com.jd.common.hbase.client.entity;

import java.util.List;

public class HBaseDataStructure {
    private String tableName;
    private String rowKey;
    private List<HBaseColumn> columns;

    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRowKey() {
        return this.rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public List<HBaseColumn> getColumns() {
        return this.columns;
    }

    public void setColumns(List<HBaseColumn> columns) {
        this.columns = columns;
    }

    public String toString() {
        return "HBaseDataStructure [tableName=" + this.tableName + ", rowKey=" + this.rowKey + "]";
    }
}

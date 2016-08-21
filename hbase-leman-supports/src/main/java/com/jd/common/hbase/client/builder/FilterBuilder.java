package com.jd.common.hbase.client.builder;

import com.jd.common.hbase.client.entity.HBaseColumn;
import com.jd.common.hbase.client.util.ReflectUtil;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.util.ArrayList;
import java.util.List;

public class FilterBuilder {
    private FilterList filterList;

    private FilterBuilder(FilterList.Operator operator) {
        if (operator == null) {
            this.filterList = new FilterList();
        } else {
            this.filterList = new FilterList(operator);
        }
    }

    public static FilterBuilder newInstance() {
        return new FilterBuilder(null);
    }

    public static FilterBuilder newInstance(FilterList.Operator operator) {
        return new FilterBuilder(operator);
    }

    public FilterBuilder addFilter(Filter filter) {
        if (filter != null) {
            this.filterList.addFilter(filter);
        }
        return this;
    }

    public FilterBuilder addFilter(Object obj, String fieldName, CompareFilter.CompareOp compareOp) {
        HBaseColumn column = ReflectUtil.getHBaseColumn(obj, fieldName);
        if ((column.getValue() != null) && (column.getValue().length > 0)) {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(column.getFamily(), column.getQualifier(), compareOp, column.getValue());

            filter.setFilterIfMissing(true);
            this.filterList.addFilter(filter);
        }
        return this;
    }

    public FilterBuilder addFilter(FilterList.Operator operator, Filter... filters) {
        List<Filter> result = new ArrayList();
        result.add(this.filterList);
        for (Filter filter : filters) {
            result.add(filter);
        }
        this.filterList = new FilterList(operator, result);
        return this;
    }

    public FilterList getFilterList() {
        return this.filterList;
    }
}

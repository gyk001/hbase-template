package com.jd.common.hbase.client.row;

import com.jd.common.hbase.exception.HBaseDataAccessException;
import com.jd.common.hbase.exception.HBaseRowMappingException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.util.ArrayList;
import java.util.List;

public class RowExtractor<T> {
    private final RowMapper<T> rowMapper;
    private Log logger = LogFactory.getLog(getClass());

    public RowExtractor(RowMapper<T> rowMapper) {
        if (rowMapper == null) {
            throw new IllegalArgumentException("RowMapper is required");
        }
        this.rowMapper = rowMapper;
    }

    public T extract(Result rs)
            throws HBaseRowMappingException {
        if ((rs == null) || (rs.isEmpty())) {
            return null;
        }
        T result = this.rowMapper.mapRow(rs);
        return result;
    }

    public List<T> extract(ResultScanner scanner)
            throws HBaseRowMappingException {
        List<T> results = new ArrayList();
        for (Result result : scanner) {
            results.add(extract(result));
        }
        return results;
    }

    public List<T> extract(Result[] rs)
            throws HBaseRowMappingException {
        List<T> results = new ArrayList();
        for (Result result : rs) {
            try {
                results.add(extract(result));
            } catch (Exception ex) {
                this.logger.error("extract error!" + ex.getMessage(), ex);
                throw new HBaseDataAccessException("execute extract error:" + result.toString(), ex);
            }
        }
        return results;
    }
}

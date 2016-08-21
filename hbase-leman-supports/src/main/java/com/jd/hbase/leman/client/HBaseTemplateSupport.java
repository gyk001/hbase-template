package com.jd.hbase.leman.client;

import com.jd.hbase.leman.client.row.RowExtractor;
import com.jd.hbase.leman.client.row.RowMapper;
import com.jd.hbase.leman.exception.HBaseDataAccessException;
import com.jd.hbase.leman.exception.HBaseRowMappingException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseTemplateSupport implements HBaseOperations {
    private Log logger = LogFactory.getLog(this.getClass());
    protected HTablePool hTablePool;

    public HBaseTemplateSupport() {
    }

    public void executePut(String tableName, Put put) throws HBaseDataAccessException {
        HTableInterface hTable = null;

        try {
            hTable = this.getTable(tableName);
            hTable.put(put);
        } catch (Exception var12) {
            throw new HBaseDataAccessException("execute put error with rowKey :" + new String(put.getRow()), var12);
        } finally {
            if(hTable != null) {
                try {
                    hTable.close();
                } catch (Exception var11) {
                    this.logger.error("", var11);
                }
            }

        }

    }

    public void executePuts(String tableName, List<Put> puts) throws HBaseDataAccessException {
        HTableInterface hTable = null;

        try {
            hTable = this.getTable(tableName);
            hTable.put(puts);
        } catch (Exception var15) {
            StringBuilder sb = new StringBuilder("{");
            Iterator i$ = puts.iterator();

            while(i$.hasNext()) {
                Put put = (Put)i$.next();
                sb.append(new String(put.getRow()) + ",");
            }

            sb.append("}");
            throw new HBaseDataAccessException("execute put error with rowKeys :" + sb.toString(), var15);
        } finally {
            if(hTable != null) {
                try {
                    hTable.flushCommits();
                    hTable.close();
                } catch (Exception var14) {
                    this.logger.error("", var14);
                }
            }

        }

    }

    public <T> T executeGet(String tableName, RowMapper<T> rowMapper, Get get) throws HBaseRowMappingException, HBaseDataAccessException {
        if(rowMapper == null) {
            return null;
        } else {
            HTableInterface hTable = this.getTable(tableName);
            Result result = null;

            T var7;
            try {
                result = hTable.get(get);
                T e = (new RowExtractor<T>(rowMapper)).extract(result);
                var7 = e;
            } catch (HBaseRowMappingException var17) {
                throw var17;
            } catch (Exception var18) {
                throw new HBaseDataAccessException("execute query error with rowKey :" + new String(get.getRow()), var18);
            } finally {
                if(hTable != null) {
                    try {
                        hTable.close();
                    } catch (Exception var16) {
                        this.logger.error("", var16);
                    }
                }

            }

            return var7;
        }
    }

    public <T> List<T> executeGets(String tableName, RowMapper<T> rowMapper, List<Get> gets) throws HBaseRowMappingException, HBaseDataAccessException {
        List resultList = null;
        HTableInterface hTable = this.getTable(tableName);

        try {
            Result[] e = hTable.get(gets);
            resultList = (new RowExtractor(rowMapper)).extract(e);
        } catch (HBaseRowMappingException var18) {
            throw var18;
        } catch (Exception var19) {
            StringBuilder sb = new StringBuilder("{");
            Iterator i$ = gets.iterator();

            while(i$.hasNext()) {
                Get get = (Get)i$.next();
                sb.append(new String(get.getRow()) + ",");
            }

            sb.append("}");
            throw new HBaseDataAccessException("execute query error with rowKeys :" + sb.toString(), var19);
        } finally {
            if(hTable != null) {
                try {
                    hTable.close();
                } catch (Exception var17) {
                    this.logger.error("", var17);
                }
            }

        }

        return resultList;
    }

    public <T> List<T> executeScan(String tableName, RowMapper<T> rowMapper, Scan scan) throws HBaseRowMappingException, HBaseDataAccessException {
        HTableInterface hTable = this.getTable(tableName);
        ResultScanner scanner = null;

        List e;
        try {
            scanner = hTable.getScanner(scan);
            e = (new RowExtractor(rowMapper)).extract(scanner);
        } catch (HBaseRowMappingException var16) {
            throw var16;
        } catch (Exception var17) {
            throw new HBaseDataAccessException("execute scan error !", var17);
        } finally {
            if(hTable != null) {
                try {
                    scanner.close();
                    hTable.close();
                } catch (Exception var15) {
                    this.logger.error("", var15);
                }
            }

        }

        return e;
    }

    public HTableInterface getTable(String tableName) {
        HTableInterface hTable = this.hTablePool.getTable(tableName);
        if(hTable == null) {
            throw new HBaseDataAccessException("can not get hTable!");
        } else {
            return hTable;
        }
    }

    public long incrementColumnValue(String tableName, String rowKey, String columnFamily, String qualifier, long v) throws HBaseDataAccessException {
        HTableInterface hTable = null;
        long val = 0L;

        try {
            hTable = this.getTable(tableName);
            val = hTable.incrementColumnValue(rowKey.getBytes(), columnFamily.getBytes(), qualifier.getBytes(), v);
        } catch (Exception var18) {
            throw new HBaseDataAccessException("execute increment Column Value error with rowKey :" + rowKey, var18);
        } finally {
            if(hTable != null) {
                try {
                    hTable.close();
                } catch (Exception var17) {
                    this.logger.error("", var17);
                }
            }

        }

        return val;
    }
}

package com.jd.hbase.leman.client;

import com.jd.hbase.leman.client.row.RowExtractor;
import com.jd.hbase.leman.client.row.RowMapper;
import com.jd.hbase.leman.exception.HBaseDataAccessException;
import com.jd.hbase.leman.exception.HBaseRowMappingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class HBaseTemplateSupport implements HBaseOperations {
    private static final Logger logger = LoggerFactory.getLogger(HBaseTemplateSupport.class);
    protected transient Connection connection;
    protected Configuration configuration;

    public HBaseTemplateSupport() {

    }

    public HBaseTemplateSupport(Configuration configuration) {
        this.configuration = configuration;
    }

    public void executePut(String tableName, Put put) throws HBaseDataAccessException {
        Table table = null;

        try {
        	table = this.getTable(tableName);
            table.put(put);
        } catch (Exception var12) {
            throw new HBaseDataAccessException("execute put error with rowKey :" + new String(put.getRow()), var12);
        } finally {
            if(table != null) {
                try {
                    table.close();
                } catch (Exception var11) {
                    this.logger.error("", var11);
                }
            }

        }

    }

    public void executePuts(String tableName, List<Put> puts) throws HBaseDataAccessException {
        Table table = null;

        try {
        	table = this.getTable(tableName);
            table.put(puts);
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
            if(table != null) {
                try {
                	// TODO: table.flushCommits();
                    table.close();
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
        	Table table = this.getTable(tableName);
            Result result = null;

            T var7;
            try {
                result = table.get(get);
                T e = (new RowExtractor<T>(rowMapper)).extract(result);
                var7 = e;
            } catch (HBaseRowMappingException var17) {
                throw var17;
            } catch (Exception var18) {
                throw new HBaseDataAccessException("execute query error with rowKey :" + new String(get.getRow()), var18);
            } finally {
                if(table != null) {
                    try {
                        table.close();
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
        Table table = this.getTable(tableName);

        try {
            Result[] e = table.get(gets);
            resultList = (new RowExtractor<T>(rowMapper)).extract(e);
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
            if(table != null) {
                try {
                    table.close();
                } catch (Exception var17) {
                    this.logger.error("", var17);
                }
            }

        }

        return resultList;
    }

    public <T> List<T> executeScan(String tableName, RowMapper<T> rowMapper, Scan scan) throws HBaseRowMappingException, HBaseDataAccessException {
        Table table = this.getTable(tableName);
        ResultScanner scanner = null;

        List e;
        try {
            scanner = table.getScanner(scan);
            e = (new RowExtractor<T>(rowMapper)).extract(scanner);
        } catch (HBaseRowMappingException var16) {
            throw var16;
        } catch (Exception var17) {
            throw new HBaseDataAccessException("execute scan error !", var17);
        } finally {
            if(table != null) {
                try {
                    scanner.close();
                    table.close();
                } catch (Exception var15) {
                    this.logger.error("", var15);
                }
            }

        }

        return e;
    }

    public Connection getConnection() throws IOException {
        if(connection!=null){
            return connection;
        }
        synchronized (HBaseTemplateSupport.class){
            if(connection!=null){
                return  connection;
            }
            return ConnectionFactory.createConnection(this.configuration);
        }
    }

    public Table getTable(String tableName) {
        Table table = null;
		try {

			table = getConnection().getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			throw new HBaseDataAccessException("can not get hTable!", e);
		}
        if(table == null) {
			throw new HBaseDataAccessException("can not get hTable!");
        }
        return table;
    }

    public long incrementColumnValue(String tableName, String rowKey, String columnFamily, String qualifier, long v) throws HBaseDataAccessException {
        Table table = null;
        long val = 0L;

        try {
            table = this.getTable(tableName);
            val = table.incrementColumnValue(rowKey.getBytes(), columnFamily.getBytes(), qualifier.getBytes(), v);
        } catch (Exception var18) {
            throw new HBaseDataAccessException("execute increment Column Value error with rowKey :" + rowKey, var18);
        } finally {
            if(table != null) {
                try {
                    table.close();
                } catch (Exception var17) {
                    this.logger.error("", var17);
                }
            }

        }

        return val;
    }


    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }
}

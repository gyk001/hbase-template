package com.jd.hbase.leman.client;

import com.jd.hbase.leman.client.row.RowMapper;
import com.jd.hbase.leman.exception.HBaseDataAccessException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;

public abstract interface HBaseOperations {
    public abstract void executePut(String paramString, Put paramPut)
            throws HBaseDataAccessException;

    public abstract void executePuts(String paramString, List<Put> paramList)
            throws HBaseDataAccessException;

    public abstract <T> T executeGet(String paramString, RowMapper<T> paramRowMapper, Get paramGet)
            throws HBaseDataAccessException;

    public abstract <T> List<T> executeGets(String paramString, RowMapper<T> paramRowMapper, List<Get> paramList)
            throws HBaseDataAccessException;

    public abstract <T> List<T> executeScan(String paramString, RowMapper<T> paramRowMapper, Scan paramScan)
            throws HBaseDataAccessException;

    public abstract long incrementColumnValue(String paramString1, String paramString2, String paramString3, String paramString4, long paramLong)
            throws HBaseDataAccessException;
}

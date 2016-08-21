package com.jd.common.hbase.client;

import com.jd.common.hbase.client.HBaseTemplateSupport;
import com.jd.common.hbase.client.builder.GetBuilder;
import com.jd.common.hbase.client.builder.PutBuilder;
import com.jd.common.hbase.client.builder.ScanBuilder;
import com.jd.common.hbase.client.entity.HBaseColumn;
import com.jd.common.hbase.client.row.BeanPropertyRowMapper;
import com.jd.common.hbase.client.row.RowMapper;
import com.jd.common.hbase.client.row.SingleColumnRowMapper;
import com.jd.common.hbase.client.util.ReflectUtil;
import com.jd.common.hbase.exception.DataParseException;
import com.jd.common.hbase.exception.HBaseDataAccessException;
import com.jd.common.hbase.exception.HBaseRowMappingException;
import com.jd.hbase.leman.annotation.Column;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTemplate extends HBaseTemplateSupport {
    public HBaseTemplate(HTablePool hTablePool) {
        this.hTablePool = hTablePool;
    }

    public void insertSingleColumn(String tableName, String rowKey, String family, String qualifier, Object value) throws HBaseDataAccessException {
        this.updateSingleColumn(tableName, rowKey, family, qualifier, value);
    }

    public void insertRow(Object obj) throws HBaseDataAccessException {
        this.updateRow(obj);
    }

    public void insertRow(String tableName, String rowKey, List<HBaseColumn> hBaseColumnList) throws HBaseDataAccessException {
        this.updateRow(tableName, rowKey, hBaseColumnList);
    }

    public void batchInsert(List<? extends Object> objs) throws DataParseException, HBaseDataAccessException {
        List puts = null;

        try {
            puts = PutBuilder.buildBatchPuts(objs);
            String e = ReflectUtil.getTableName(objs.get(0));
            this.executePuts(e, puts);
        } catch (HBaseDataAccessException var4) {
            throw var4;
        } catch (Exception var5) {
            throw new DataParseException(var5.getMessage(), var5);
        }
    }

    public void updateSingleColumn(String tableName, String rowKey, String family, String qualifier, Object value) throws DataParseException, HBaseDataAccessException {
        try {
            if(tableName == null) {
                throw new IllegalArgumentException("hbase table should not be null!");
            } else if(rowKey == null) {
                throw new IllegalArgumentException("rowKey should not be null!");
            } else if(family != null && qualifier != null) {
                Put e = PutBuilder.build(rowKey, family, qualifier, value);
                ArrayList puts = new ArrayList();
                puts.add(e);
                this.executePuts(tableName, puts);
            } else {
                throw new IllegalArgumentException("no row found!");
            }
        } catch (HBaseDataAccessException var8) {
            throw var8;
        } catch (Exception var9) {
            throw new DataParseException(var9);
        }
    }

    public void updateRow(Object obj) throws DataParseException, HBaseDataAccessException {
        try {
            List e = PutBuilder.build(obj);
            this.executePuts(ReflectUtil.getTableName(obj.getClass()), e);
        } catch (HBaseDataAccessException var3) {
            throw var3;
        } catch (Exception var4) {
            throw new DataParseException(var4);
        }
    }

    public void updateRow(String tableName, String rowKey, List<HBaseColumn> hBaseColumnList) throws HBaseDataAccessException {
        try {
            List e = PutBuilder.build(rowKey, hBaseColumnList);
            this.executePuts(tableName, e);
        } catch (HBaseDataAccessException var5) {
            throw var5;
        } catch (Exception var6) {
            throw new DataParseException(var6);
        }
    }

    public void delete(Object obj) throws HBaseDataAccessException {
        this.delete(ReflectUtil.getTableName(obj.getClass()), ReflectUtil.getRowKey(obj));
    }

    public void delete(String tableName, String rowKey) throws HBaseDataAccessException {
        try {
            HTableInterface e = this.getTable(tableName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            e.delete(delete);
        } catch (Exception var5) {
            throw new HBaseDataAccessException("execute delete error with rowKey :" + new String(rowKey), var5);
        }
    }

    public <T> T queryForRow(T obj) throws Exception {
        return this.queryForRow(obj, (Class<T>)obj.getClass());
    }

    public <T> T queryForRow(T obj, Class<T> requiredType) throws Exception {
        T t = null;

        try {
            t = this.queryForRow(ReflectUtil.getTableName(obj.getClass()), ReflectUtil.getRowKey(obj), requiredType);
            return t;
        } catch (HBaseDataAccessException var5) {
            throw var5;
        } catch (HBaseRowMappingException var6) {
            throw var6;
        } catch (Exception var7) {
            throw new DataParseException(var7);
        }
    }

    public <T> T queryForRow(String tableName, String rowKey, Class<T> requiredType) throws Exception {
        return this.queryForRow(tableName, rowKey, (RowMapper<T>)(new BeanPropertyRowMapper(requiredType)));
    }

    public <T> T queryForRow(String tableName, String rowKey, RowMapper<T> rowMapper) throws Exception {
        try {
            Get e = GetBuilder.build(rowKey);
            return this.executeGet(tableName, rowMapper, e);
        } catch (HBaseDataAccessException var5) {
            throw var5;
        } catch (HBaseRowMappingException var6) {
            throw var6;
        } catch (Exception var7) {
            throw new DataParseException(var7);
        }
    }

    public <T> T querySingleColumnInRow(Object obj, String fieldName, Class<T> requiredType) throws Exception {
        try {
            String e = null;
            String tableName = ReflectUtil.getTableName(obj);
            e = ReflectUtil.getRowKey(obj);
            Column column = ReflectUtil.getColumn(obj.getClass(), fieldName);
            return this.querySingleColumnInRow(tableName, e, column.family(), column.name(), requiredType);
        } catch (HBaseDataAccessException var7) {
            throw var7;
        } catch (HBaseRowMappingException var8) {
            throw var8;
        } catch (Exception var9) {
            throw new DataParseException(var9);
        }
    }

    public <T> T querySingleColumnInRow(String tableName, String rowKey, String family, String qualifier, Class<T> requiredType) throws Exception {
        try {
            Get e = GetBuilder.build(rowKey, family, qualifier);
            return this.executeGet(tableName, new SingleColumnRowMapper<T>(family, qualifier, requiredType), e);
        } catch (HBaseDataAccessException var7) {
            throw var7;
        } catch (HBaseRowMappingException var8) {
            throw var8;
        } catch (Exception var9) {
            throw new DataParseException(var9);
        }
    }

    public <T> T queryMultiColumnInRow(String tableName, String rowKey, List<HBaseColumn> columnList, Class<T> requiredType) throws Exception {
        try {
            Get e = GetBuilder.build(rowKey, columnList);
            return this.executeGet(tableName, new BeanPropertyRowMapper<T>(requiredType), e);
        } catch (HBaseDataAccessException var6) {
            throw var6;
        } catch (HBaseRowMappingException var7) {
            throw var7;
        } catch (Exception var8) {
            throw new DataParseException(var8);
        }
    }

    public <T> T queryFamilyAllColumnsInRow(Object obj, Class<T> requiredType, String... families) throws Exception {
        try {
            return this.queryFamilyAllColumnsInRow(ReflectUtil.getTableName(obj), ReflectUtil.getRowKey(obj), requiredType, families);
        } catch (HBaseDataAccessException var5) {
            throw var5;
        } catch (HBaseRowMappingException var6) {
            throw var6;
        } catch (Exception var7) {
            throw new DataParseException(var7);
        }
    }

    public <T> T queryFamilyAllColumnsInRow(String tableName, String rowKey, Class<T> requiredType, String... families) {
        try {
            Get e = GetBuilder.build(rowKey, families);
            return this.executeGet(tableName, new BeanPropertyRowMapper<T>(requiredType), e);
        } catch (HBaseDataAccessException var6) {
            throw var6;
        } catch (HBaseRowMappingException var7) {
            throw var7;
        } catch (Exception var8) {
            throw new DataParseException(var8);
        }
    }

    public <T> List<T> scanAllColumn(Object obj, Class<T> requiredType) throws Exception {
        return this.scanAllColumn(obj, requiredType, new BeanPropertyRowMapper(requiredType));
    }

    public <T> List<T> scanAllColumnByGetList(Object obj, Class<T> requiredType, List<String> gets) throws Exception {
        String tableName = ReflectUtil.getTableName(obj.getClass());

        try {
            ArrayList e = new ArrayList();
            Iterator i$ = gets.iterator();

            while(i$.hasNext()) {
                String get = (String)i$.next();
                e.add(new Get(get.getBytes()));
            }

            return super.executeGets(tableName, new BeanPropertyRowMapper(requiredType), e);
        } catch (HBaseDataAccessException var8) {
            throw var8;
        } catch (HBaseRowMappingException var9) {
            throw var9;
        } catch (Exception var10) {
            throw new DataParseException(var10);
        }
    }

    public <T> List<T> scanAllColumnByGetList(Object obj, Class<T> requiredType, RowMapper<T> rowMapper, List<String> gets) throws Exception {
        String tableName = ReflectUtil.getTableName(obj.getClass());

        try {
            ArrayList e = new ArrayList();
            Iterator i$ = gets.iterator();

            while(i$.hasNext()) {
                String get = (String)i$.next();
                e.add(new Get(get.getBytes()));
            }

            return super.executeGets(tableName, rowMapper, e);
        } catch (HBaseDataAccessException var9) {
            throw var9;
        } catch (HBaseRowMappingException var10) {
            throw var10;
        } catch (Exception var11) {
            throw new DataParseException(var11);
        }
    }

    public <T> List<T> scanAllColumnByPage(Object obj, Class<T> requiredType, FilterList filterList, String startRow, String stopRow) throws Exception {
        String tableName = ReflectUtil.getTableName(obj.getClass());

        try {
            Scan e = ScanBuilder.build(obj);
            e.setStartRow(startRow.getBytes());
            e.setStopRow(stopRow.getBytes());
            e.setFilter(filterList);
            return this.executeScan(tableName, new BeanPropertyRowMapper(requiredType), e);
        } catch (HBaseDataAccessException var8) {
            throw var8;
        } catch (HBaseRowMappingException var9) {
            throw var9;
        } catch (Exception var10) {
            throw new DataParseException(var10);
        }
    }

    public <T> List<T> scanAllColumn(Object obj, Class<T> requiredType, RowMapper<T> rowMapper) throws Exception {
        String tableName = ReflectUtil.getTableName(obj.getClass());

        try {
            Scan e = ScanBuilder.build(obj);
            return this.executeScan(tableName, rowMapper, e);
        } catch (HBaseDataAccessException var6) {
            throw var6;
        } catch (HBaseRowMappingException var7) {
            throw var7;
        } catch (Exception var8) {
            throw new DataParseException(var8);
        }
    }

    public <T> List<T> queryForListWithAllColumns(Class<T> requiredType, FilterList filterList) throws Exception {
        return this.queryForListWithAllColumns(requiredType, filterList, new BeanPropertyRowMapper(requiredType));
    }

    public <T> List<T> queryForListWithAllColumns(Class<T> requiredType, FilterList filterList, RowMapper<T> rowMapper) throws Exception {
        try {
            Scan e = ScanBuilder.build(requiredType);
            e.setFilter(filterList);
            return this.executeScan(ReflectUtil.getTableName(requiredType), rowMapper, e);
        } catch (HBaseDataAccessException var5) {
            throw var5;
        } catch (HBaseRowMappingException var6) {
            throw var6;
        } catch (Exception var7) {
            throw new DataParseException(var7);
        }
    }

    public <T> List<T> queryForListWithColumns(Class<T> requiredType, List<HBaseColumn> columnList, FilterList filterList) throws Exception {
        try {
            return this.queryForListWithColumns(requiredType, columnList, filterList, new BeanPropertyRowMapper(requiredType));
        } catch (HBaseDataAccessException var5) {
            throw var5;
        } catch (HBaseRowMappingException var6) {
            throw var6;
        } catch (Exception var7) {
            throw new DataParseException(var7);
        }
    }

    public <T> List<T> queryForListWithColumns(Class<T> requiredType, List<HBaseColumn> columnList, FilterList filterList, RowMapper<T> rowMapper) throws HBaseDataAccessException {
        try {
            Scan e = ScanBuilder.build(columnList);
            e.setFilter(filterList);
            return this.executeScan(ReflectUtil.getTableName(requiredType), rowMapper, e);
        } catch (HBaseDataAccessException var6) {
            throw var6;
        } catch (HBaseRowMappingException var7) {
            throw var7;
        } catch (Exception var8) {
            throw new DataParseException(var8);
        }
    }

    public <T> List<T> queryForListWithFamilies(Class<T> requiredType, FilterList filterList, String... families) throws Exception {
        return this.queryForListWithFamilies(requiredType, filterList, new BeanPropertyRowMapper(requiredType), families);
    }

    public <T> List<T> queryForListWithFamilies(Class<T> requiredType, FilterList filterList, RowMapper<T> rowMapper, String... families) throws Exception {
        try {
            Scan e = ScanBuilder.build(families);
            e.setFilter(filterList);
            return this.executeScan(ReflectUtil.getTableName(requiredType), rowMapper, e);
        } catch (HBaseDataAccessException var6) {
            throw var6;
        } catch (HBaseRowMappingException var7) {
            throw var7;
        } catch (Exception var8) {
            throw new DataParseException(var8);
        }
    }
}

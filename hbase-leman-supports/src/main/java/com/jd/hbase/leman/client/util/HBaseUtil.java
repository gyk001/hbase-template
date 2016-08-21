//package com.jd.hbase.leman.client.util;
//
//import org.apache.hadoop.hbase.client.HTableInterface;
//import org.apache.hadoop.hbase.client.HTablePool;
//
//import java.lang.reflect.Field;
//import java.util.LinkedList;
//import java.util.concurrent.ConcurrentMap;
//
//public final class HBaseUtil {
////    public static void closeHTablePool(HTablePool pool)
////            throws Exception {
////        if (pool == null) {
////            return;
////        }
////        Field field = HTablePool.class.getField("tables");
////        field.setAccessible(true);
////
////        ConcurrentMap<String, LinkedList<HTableInterface>> tables = (ConcurrentMap) field.get(pool);
////        for (String table : tables.keySet()) {
////            pool.closeTablePool(table);
////        }
////    }
//}

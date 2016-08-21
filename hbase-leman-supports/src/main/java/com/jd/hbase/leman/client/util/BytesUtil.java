package com.jd.hbase.leman.client.util;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;

public class BytesUtil {
    public static byte[] toBytes(Class<?> clazz, Object obj) {
        if ((obj == null) || (clazz == null)) {
            return null;
        }
        if (isCharacter(clazz)) {
            return Bytes.toBytes(String.valueOf(((Character) obj).charValue()));
        }
        if (isString(clazz)) {
            return Bytes.toBytes((String) obj);
        }
        if (isBool(clazz)) {
            Boolean value = (Boolean) obj;
            if ((Boolean.TYPE == clazz) && (!value.booleanValue())) {
                return null;
            }
            return Bytes.toBytes(value.booleanValue());
        }
        if (isByte(clazz)) {
            return Bytes.toBytes(((Byte) obj).shortValue());
        }
        if (isShort(clazz)) {
            return Bytes.toBytes(((Short) obj).shortValue());
        }
        if (isInt(clazz)) {
            return Bytes.toBytes(((Integer) obj).intValue());
        }
        if (isLong(clazz)) {
            return Bytes.toBytes(((Long) obj).longValue());
        }
        if (isFloat(clazz)) {
            return Bytes.toBytes(((Float) obj).floatValue());
        }
        if (isDouble(clazz)) {
            return Bytes.toBytes(((Double) obj).doubleValue());
        }
        if (byte[].class.equals(clazz)) {
            return (byte[]) obj;
        }
        if (Date.class.equals(clazz)) {
            return Bytes.toBytes(((Date) obj).getTime());
        }
        if (clazz.isEnum()) {
            return Bytes.toBytes(((Enum) obj).name());
        }
        return null;
    }

    public static byte[] stringToBytes(String value) {
        return Bytes.toBytes(value);
    }

    public static byte[] toBytes(Object obj) {
        if (obj != null) {
            return toBytes(obj.getClass(), obj);
        }
        return null;
    }

    public static Object toObject(byte[] b, Class clazz) {
        if (clazz == null) {
            return b;
        }
        if ((b == null) || (b.length == 0)) {
            return null;
        }
        if (isCharacter(clazz)) {
            return Character.valueOf(Bytes.toString(b).charAt(0));
        }
        if (isString(clazz)) {
            return Bytes.toString(b);
        }
        if (isBool(clazz)) {
            return Boolean.valueOf(Bytes.toBoolean(b));
        }
        if (isByte(clazz)) {
            if (b.length == 1) {
                return Byte.valueOf(b[0]);
            }
            return Byte.valueOf((byte) Bytes.toShort(b));
        }
        if (isShort(clazz)) {
            return Short.valueOf(Bytes.toShort(b));
        }
        if (isInt(clazz)) {
            return Integer.valueOf(Bytes.toInt(b));
        }
        if (isLong(clazz)) {
            return Long.valueOf(Bytes.toLong(b));
        }
        if (isFloat(clazz)) {
            return Float.valueOf(Bytes.toFloat(b));
        }
        if ((isDouble(clazz)) || (isNumber(clazz))) {
            return Double.valueOf(Bytes.toDouble(b));
        }
        if (byte[].class.equals(clazz)) {
            return b;
        }
        if (Date.class.equals(clazz)) {
            return new Date(Bytes.toLong(b));
        }
        if (clazz.isEnum()) {
        	 return Enum.valueOf(clazz, Bytes.toString(b));
        }
        return b;
    }

    private static boolean isNumber(Class<?> clazz) {
        return Number.class.isAssignableFrom(clazz);
    }

    private static boolean isByte(Class<?> clazz) {
        return (clazz == Byte.TYPE) || (clazz == Byte.class);
    }

    private static boolean isBool(Class<?> clazz) {
        return (clazz == Boolean.TYPE) || (clazz == Boolean.class);
    }

    private static boolean isInt(Class<?> clazz) {
        return (clazz == Integer.TYPE) || (clazz == Integer.class);
    }

    private static boolean isLong(Class<?> clazz) {
        return (clazz == Long.TYPE) || (clazz == Long.class);
    }

    private static boolean isDouble(Class<?> clazz) {
        return (clazz == Double.TYPE) || (clazz == Double.class);
    }

    private static boolean isFloat(Class<?> clazz) {
        return (clazz == Float.TYPE) || (clazz == Float.class);
    }

    private static boolean isCharacter(Class<?> clazz) {
        return (clazz == Character.TYPE) || (clazz == Character.class);
    }

    private static boolean isString(Class<?> clazz) {
        return clazz == String.class;
    }

    private static boolean isShort(Class<?> clazz) {
        return (clazz == Short.TYPE) || (clazz == Short.class);
    }
}

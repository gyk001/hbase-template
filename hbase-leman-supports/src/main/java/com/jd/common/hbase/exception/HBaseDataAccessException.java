package com.jd.common.hbase.exception;

public class HBaseDataAccessException
        extends RuntimeException {
    private static final long serialVersionUID = 934005954825532796L;

    public HBaseDataAccessException(String msg) {
        super(msg);
    }

    public HBaseDataAccessException(String msg, Throwable cause) {
        super(msg, cause);
    }
}

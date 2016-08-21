package com.jd.common.hbase.exception;

public class HBaseRowMappingException
        extends RuntimeException {
    private static final long serialVersionUID = 8841916508020237031L;

    public HBaseRowMappingException(String msg) {
        super(msg);
    }

    public HBaseRowMappingException(String msg, Throwable cause) {
        super(msg, cause);
    }
}

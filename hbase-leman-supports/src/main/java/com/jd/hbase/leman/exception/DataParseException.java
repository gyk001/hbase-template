package com.jd.hbase.leman.exception;

public class DataParseException
        extends RuntimeException {
    private static final long serialVersionUID = 4670682745997533204L;

    public DataParseException(String msg) {
        super(msg);
    }

    public DataParseException(Throwable cause) {
        super(cause);
    }

    public DataParseException(String msg, Throwable cause) {
        super(msg, cause);
    }
}

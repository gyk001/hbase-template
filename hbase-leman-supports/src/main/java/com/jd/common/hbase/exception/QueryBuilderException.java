package com.jd.common.hbase.exception;

public class QueryBuilderException
        extends RuntimeException {
    private static final long serialVersionUID = 934005954825532796L;

    public QueryBuilderException(String msg) {
        super(msg);
    }

    public QueryBuilderException(String msg, Throwable cause) {
        super(msg, cause);
    }
}

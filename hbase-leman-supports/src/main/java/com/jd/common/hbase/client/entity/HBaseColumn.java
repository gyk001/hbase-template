package com.jd.common.hbase.client.entity;

public class HBaseColumn {
    private byte[] family;
    private byte[] qualifier;
    private byte[] value;

    public byte[] getFamily() {
        return this.family;
    }

    public void setFamily(byte[] family) {
        this.family = family;
    }

    public byte[] getQualifier() {
        return this.qualifier;
    }

    public void setQualifier(byte[] qualifier) {
        this.qualifier = qualifier;
    }

    public byte[] getValue() {
        return this.value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}

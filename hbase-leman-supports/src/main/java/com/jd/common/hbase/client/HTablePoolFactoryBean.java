package com.jd.common.hbase.client;

import com.jd.common.hbase.client.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

public class HTablePoolFactoryBean
        implements FactoryBean, InitializingBean, DisposableBean {
    private Configuration config;
    private Integer maxSize;
    private HTablePool hTablePool;

    public void setConfig(Configuration config) {
        this.config = config;
    }

    public void setMaxSize(Integer maxSize) {
        this.maxSize = maxSize;
    }

    public void afterPropertiesSet()
            throws Exception {
        this.hTablePool = new HTablePool(this.config, this.maxSize == null ? Integer.MAX_VALUE : this.maxSize.intValue());
    }

    public HTablePool getObject()
            throws Exception {
        return this.hTablePool;
    }

    public Class<?> getObjectType() {
        return HTablePool.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public void destroy()
            throws Exception {
        HBaseUtil.closeHTablePool(this.hTablePool);
    }
}

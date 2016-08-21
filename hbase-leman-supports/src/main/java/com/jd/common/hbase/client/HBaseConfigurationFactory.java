package com.jd.common.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Properties;
import java.util.Set;

public class HBaseConfigurationFactory {
    private Properties configuration;

    public Configuration createHBaseConfiguration() {
        if (this.configuration != null) {
            Configuration config = HBaseConfiguration.create();
            Set<String> propertyNames = this.configuration.stringPropertyNames();
            for (String propertyName : propertyNames) {
                config.set(propertyName, this.configuration.getProperty(propertyName));
            }
            return config;
        }
        throw new RuntimeException("hbase properties cannot be null");
    }

    public void setConfiguration(Properties configuration) {
        this.configuration = configuration;
    }
}

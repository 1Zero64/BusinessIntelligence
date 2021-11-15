package de.hfu.businessintelligence.configuration;

import org.apache.spark.sql.SparkSession;

public class SparkConfiguration {

    private static final String APP_NAME = "BusinessIntelligence";
    private static final String THRIFT_SERVER_PORT_CONFIG_PROP = "hive.server2.thrift.port";
    private static final String THRIFT_SERVER_PORT = "10000";

    private static final SparkConfiguration instance = new SparkConfiguration();

    private SparkConfiguration() {

    }

    public static SparkConfiguration getInstance() {
        return instance;
    }

    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName(APP_NAME)
                .config(THRIFT_SERVER_PORT_CONFIG_PROP, THRIFT_SERVER_PORT)
                .enableHiveSupport()
                .getOrCreate();
    }
}

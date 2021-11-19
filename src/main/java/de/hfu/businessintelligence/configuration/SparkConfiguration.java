package de.hfu.businessintelligence.configuration;

import org.apache.spark.sql.SparkSession;

public class SparkConfiguration {

    private static final String APP_NAME = "BusinessIntelligence";

    private static final SparkConfiguration instance = new SparkConfiguration();

    private SparkConfiguration() {

    }

    public static SparkConfiguration getInstance() {
        return instance;
    }

    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName(APP_NAME)
                .enableHiveSupport()
                .getOrCreate();
    }
}

package de.hfu.businessintelligence.configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkConfiguration {

    private static final String CLUSTER_URL = "local";
    private static final String APP_NAME = "BusinessIntelligence";

    private static final SparkConfiguration instance = new SparkConfiguration();

    private SparkConfiguration() {

    }

    public static SparkConfiguration getInstance() {
        return instance;
    }

    public SparkSession sparkSession() {
        return SparkSession.builder()
                .sparkContext(sc().sc())
                .appName(APP_NAME)
                .getOrCreate();
    }

    private JavaSparkContext sc() {
        return new JavaSparkContext(sparkConf());
    }

    private SparkConf sparkConf() {
        return new SparkConf()
                .setMaster(CLUSTER_URL)
                .setAppName(APP_NAME);
    }
}

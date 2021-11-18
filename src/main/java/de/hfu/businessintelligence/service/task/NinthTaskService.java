package de.hfu.businessintelligence.service.task;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.TableConfiguration.PASSENGER_COUNT_COLUMN;
import static de.hfu.businessintelligence.configuration.TableConfiguration.TRIPS_TABLE;

public class NinthTaskService implements TaskService {

    private volatile static NinthTaskService instance;

    private final SparkSession sparkSession;

    private NinthTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static NinthTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (NinthTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new NinthTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        getAvgPassengersInTaxi().write().mode(SaveMode.Overwrite).saveAsTable("avgPassengersCount");
    }

    private Dataset<Row> getAvgPassengersInTaxi() {
        String statement = buildAvgPassengerSqlStatement();
        return sparkSession.sql(statement);
    }

    private Dataset<Row> countSameTrips(double distanceToleranceInKilometers, long timeToleranceInSeconds) {
        return sparkSession.sql("select * from trips");
    }

    private Dataset<Row> countTotalTrips() {
        return sparkSession.sql("SELECT COUNT(*) FROM trips");
    }

    private String buildAvgPassengerSqlStatement() {
        return "SELECT AVG("
                .concat(PASSENGER_COUNT_COLUMN)
                .concat(") as avgPassengerCount FROM ")
                .concat(TRIPS_TABLE);
    }
}

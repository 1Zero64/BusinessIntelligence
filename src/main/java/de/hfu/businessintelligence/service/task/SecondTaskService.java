package de.hfu.businessintelligence.service.task;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.TableConfiguration.PICK_UP_LATITUDE_COLUMN;
import static de.hfu.businessintelligence.configuration.TableConfiguration.TRIPS_TABLE;

public class SecondTaskService implements TaskService {

    private volatile static SecondTaskService instance;

    private final SparkSession sparkSession;

    private SecondTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static SecondTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (SecondTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new SecondTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        countTripsWithLatitudeGreaterOrSmallerThan(40.711109d, true).write().mode(SaveMode.Overwrite).saveAsTable("countedTripsInNorth");
        countTripsWithLatitudeGreaterOrSmallerThan(40.711109d, false).write().mode(SaveMode.Overwrite).saveAsTable("countedTripsInSouth");
        countTripsWithLatitudeGreaterOrSmallerThan(-90.1, true).write().mode(SaveMode.Overwrite).saveAsTable("countedTripsInTotal");
    }

    private Dataset<Row> countTripsWithLatitudeGreaterOrSmallerThan(double latitude, boolean isGreater) {
        String statement = buildSqlStatementWith(latitude, isGreater);
        return sparkSession.sql(statement);
    }

    private String buildSqlStatementWith(double latitude, boolean isGreater) {
        return "SELECT COUNT(*) as countedTrips from "
                .concat(TRIPS_TABLE)
                .concat(" WHERE ")
                .concat(PICK_UP_LATITUDE_COLUMN)
                .concat(" ")
                .concat(isGreater ? ">" : "<")
                .concat(" ")
                .concat(String.valueOf(latitude));
    }
}

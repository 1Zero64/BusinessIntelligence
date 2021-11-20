package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.*;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;
import static de.hfu.businessintelligence.configuration.TableConfiguration.*;

public class SeventhTaskService implements TaskService {

    private volatile static SeventhTaskService instance;

    private final SparkSession sparkSession;

    private SeventhTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static SeventhTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (SeventhTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new SeventhTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        if (USE_CSV_OUTPUT) {
            FileService.getInstance().saveAsCsvFile(getAvgTipAmountGroupedByTripDistance(), "avgTipAmountInDollarsGroupedByTripDistanceInKilometers");
        } else {
            getAvgTipAmountGroupedByTripDistance().write().mode(SaveMode.Overwrite).saveAsTable("avgTipAmountInDollarsGroupedByTripDistanceInKilometers");
        }
    }

    private Dataset<Row> getAvgTipAmountGroupedByTripDistance() {
        String statement = buildSqlStatement();
        return sparkSession.sql(statement)
                .sort(functions.asc("tripDistanceInKilometers"));
    }

    private String buildSqlStatement() {
        return "WITH newTrips as (SELECT ROUND("
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(" * 1.60934, 0) as tripDistanceInKilometers, AVG(")
                .concat(TIP_AMOUNT_COLUMN)
                .concat(") as avgTipAmountInDollars")
                .concat(" FROM ")
                .concat(TRIPS_TABLE)
                .concat(" GROUP BY tripDistanceInKilometers)")
                .concat(" SELECT ")
                .concat("tripDistanceInKilometers, avgTipAmountInDollars, (avgTipAmountInDollars / tripDistanceInKilometers) as avgTipPerKilometer")
                .concat(" FROM ")
                .concat("newTrips");
    }
}

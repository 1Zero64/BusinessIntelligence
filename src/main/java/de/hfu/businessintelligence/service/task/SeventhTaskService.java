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
                .sort(functions.asc("tripDistanceIntervalInKilometers"));
    }

    private String buildSqlStatement() {
        return "WITH newTrips as (SELECT CEIL(ROUND("
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(" * 1.60934, 0) / 5) * 5 as tripDistanceIntervalInKilometers, AVG(")
                .concat(TIP_AMOUNT_COLUMN)
                .concat(") as avgTipAmount")
                .concat(" FROM ")
                .concat(TRIPS_TABLE)
                .concat(" GROUP BY ")
                .concat("tripDistanceIntervalInKilometers")
                .concat(" HAVING ")
                .concat("tripDistanceIntervalInKilometers BETWEEN 1 AND 100)")
                .concat(" SELECT ")
                .concat("tripDistanceIntervalInKilometers, avgTipAmount, (avgTipAmount / tripDistanceIntervalInKilometers) as avgTipPerKilometer ")
                .concat(" FROM ")
                .concat("newTrips");
    }
}

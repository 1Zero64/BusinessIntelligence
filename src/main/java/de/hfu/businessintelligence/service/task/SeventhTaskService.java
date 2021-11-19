package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.*;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;
import static de.hfu.businessintelligence.configuration.TableConfiguration.*;
import static de.hfu.businessintelligence.configuration.UnitConfiguration.MILES_TO_KILOMETERS;

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
                .sort(functions.asc(TRIP_DISTANCE_COLUMN));
    }

    private String buildSqlStatement() {
        return "SELECT ROUND("
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(" * ")
                .concat(String.valueOf(MILES_TO_KILOMETERS))
                .concat(", 0) as ")
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(", AVG(")
                .concat(TIP_AMOUNT_COLUMN)
                .concat(") as avgTipAmountInDollars")
                .concat(", (avgTipAmountInDollars / ")
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(") as tipPerKM")
                .concat(" FROM")
                .concat(TRIPS_TABLE)
                .concat(" GROUP BY ROUND(")
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(" * ")
                .concat(String.valueOf(MILES_TO_KILOMETERS))
                .concat(", 0)");
    }
}

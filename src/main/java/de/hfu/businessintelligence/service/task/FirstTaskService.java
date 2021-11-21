package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;
import static de.hfu.businessintelligence.configuration.TableConfiguration.*;
import static de.hfu.businessintelligence.configuration.UnitConfiguration.KILOMETERS_TO_MILES;
import static de.hfu.businessintelligence.configuration.UnitConfiguration.MILES_TO_KILOMETERS;

public class FirstTaskService implements TaskService {

    private volatile static FirstTaskService instance;

    private final SparkSession sparkSession;

    private FirstTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static FirstTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (FirstTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new FirstTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        if (USE_CSV_OUTPUT) {
            FileService.getInstance().saveAsCsvFile(getAverageIncomeBetween(0, Double.MAX_VALUE), "averageIncomeInDollars");
            FileService.getInstance().saveAsCsvFile(getAverageIncomeBetween(0, 30), "averageIncomeInDollarsBetween0And30Kilometers");
            FileService.getInstance().saveAsCsvFile(getAverageIncomeBetween(30, 50), "averageIncomeInDollarsBetween30And50Kilometers");
            FileService.getInstance().saveAsCsvFile(getCountedTripsBetween(), "countedTripsBetween");
        } else {
            getAverageIncomeBetween(0, Double.MAX_VALUE).write().mode(SaveMode.Overwrite).saveAsTable("averageIncomeInDollars");
            getAverageIncomeBetween(0, 30).write().mode(SaveMode.Overwrite).saveAsTable("averageIncomeInDollarsBetween0And30Kilometers");
            getAverageIncomeBetween(30, 50).write().mode(SaveMode.Overwrite).saveAsTable("averageIncomeInDollarsBetween30And50Kilometers");
            getCountedTripsBetween().write().mode(SaveMode.Overwrite).saveAsTable("countedTripsBetween");
        }
    }

    public Dataset<Row> getAverageIncomeBetween(double minTripDistanceInKilometers, double maxTripDistanceInKilometers) {
        String statement = buildSqlStatementWith(minTripDistanceInKilometers, maxTripDistanceInKilometers);
        return sparkSession.sql(statement);
    }

    public Dataset<Row> getCountedTripsBetween() {
        String statement = buildSqlStatementCountedTripsBetween();
        return sparkSession.sql(statement);
    }

    private String buildSqlStatementWith(double minTripDistanceInKilometers, double maxTripDistanceInKilometers) {
        double minTripDistanceInMiles = minTripDistanceInKilometers * KILOMETERS_TO_MILES;
        double maxTripDistanceInMiles = maxTripDistanceInKilometers * KILOMETERS_TO_MILES;

        return "select AVG("
                .concat(TOTAL_AMOUNT_COLUMN)
                .concat(") as avgTotalAmountInDollars FROM ")
                .concat(TRIPS_TABLE)
                .concat(" WHERE ")
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(" > ")
                .concat(String.valueOf(minTripDistanceInMiles))
                .concat(" AND ")
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(" <= ")
                .concat(String.valueOf(maxTripDistanceInMiles))
                .concat(" AND ")
                .concat(TOTAL_AMOUNT_COLUMN)
                .concat(" < 250");
    }

    private String buildSqlStatementCountedTripsBetween() {
        return "SELECT SUM(CASE WHEN ("
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(" * ")
                .concat(String.valueOf(MILES_TO_KILOMETERS))
                .concat(") <= 30 THEN 1 ELSE 0 END) as countedTripsBetween0And30Kilometers, ")
                .concat("SUM(CASE WHEN (")
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(" * ")
                .concat(String.valueOf(MILES_TO_KILOMETERS))
                .concat(") BETWEEN 31 AND 50 THEN 1 ELSE 0 END) as countedTripsBetween30And50Kilometers, ")
                .concat("SUM(CASE WHEN (")
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(" * ")
                .concat(String.valueOf(MILES_TO_KILOMETERS))
                .concat(") > 50 THEN 1 ELSE 0 END) as countedTripsOver50Kilometers")
                .concat(" FROM ")
                .concat(TRIPS_TABLE);
    }
}

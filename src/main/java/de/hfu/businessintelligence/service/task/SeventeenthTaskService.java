package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;
import static de.hfu.businessintelligence.configuration.TableConfiguration.*;
import static de.hfu.businessintelligence.configuration.UnitConfiguration.MILES_TO_KILOMETERS;

public class SeventeenthTaskService implements TaskService {

    private volatile static SeventeenthTaskService instance;

    private final SparkSession sparkSession;

    private SeventeenthTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static SeventeenthTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (SeventeenthTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new SeventeenthTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        if (USE_CSV_OUTPUT) {
            FileService.getInstance().saveAsCsvFile(getTotalAmountPerKmInDollar(), "totalAmountInDollarPerKmGroupedByRateCode");
            FileService.getInstance().saveAsCsvFile(getAvgDistanceGroupedByRateCode(), "avgDistanceInKilometersGroupedByRateCode");
        } else {
            getTotalAmountPerKmInDollar().write().mode(SaveMode.Overwrite).saveAsTable("totalAmountInDollarPerKmGroupedByRateCode");
            getAvgDistanceGroupedByRateCode().write().mode(SaveMode.Overwrite).saveAsTable("avgDistanceInKilometersGroupedByRateCode");
        }
    }

    private Dataset<Row> getTotalAmountPerKmInDollar() {
        String statement = buildTotalAmountPerKmInDollarStatement();
        return sparkSession.sql(statement);
    }

    private Dataset<Row> getAvgDistanceGroupedByRateCode() {
        String statement = buildAvgDistanceGroupedByRateCodeStatement();
        return sparkSession.sql(statement);
    }

    private String buildTotalAmountPerKmInDollarStatement() {
        return "SELECT "
                .concat(RATE_CODE_COLUMN)
                .concat(", AVG(")
                .concat(TOTAL_AMOUNT_COLUMN)
                .concat("/ (")
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(" * 1.60934)) as totalAmountInDollarPerKm FROM ")
                .concat(TRIPS_TABLE)
                .concat(" WHERE (")
                .concat(RATE_CODE_COLUMN)
                .concat(" = 0 OR ")
                .concat(RATE_CODE_COLUMN)
                .concat(" = 5) AND ")
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(" > 0 GROUP BY ")
                .concat(RATE_CODE_COLUMN);
    }

    private String buildAvgDistanceGroupedByRateCodeStatement() {
        return "SELECT rateCode, AVG("
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(" * ")
                .concat(String.valueOf(MILES_TO_KILOMETERS))
                .concat(") as avgTripDistanceInKilometers FROM ")
                .concat(TRIPS_TABLE)
                .concat(" GROUP BY ")
                .concat(RATE_CODE_COLUMN);
    }
}

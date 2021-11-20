package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;
import static de.hfu.businessintelligence.configuration.TableConfiguration.*;

public class SixteenthTaskService implements TaskService {

    private volatile static SixteenthTaskService instance;

    private final SparkSession sparkSession;

    private SixteenthTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static SixteenthTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (SixteenthTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new SixteenthTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        if (USE_CSV_OUTPUT) {
            FileService.getInstance().saveAsCsvFile(getCountedUnknownTripsPerVendor(), "countedUnknownTripsPerVendor");
        } else {
            getCountedUnknownTripsPerVendor().write().mode(SaveMode.Overwrite).saveAsTable("countedUnknownTripsPerVendor");
        }
    }

    private Dataset<Row> getCountedUnknownTripsPerVendor() {
        String statement = buildStatement();
        return sparkSession.sql(statement);
    }

    private String buildStatement() {
        return "SELECT "
                .concat(VENDOR_ID_COLUMN)
                .concat(", SUM(CASE WHEN ")
                .concat(PAYMENT_TYPE_COLUMN)
                .concat(" = 'UNK' THEN 1 ELSE 0 END) as countedUnknownTrips, SUM(CASE WHEN ")
                .concat(PAYMENT_TYPE_COLUMN)
                .concat(" <> 'UNK' THEN 1 ELSE 0 END) as countedKnownTrips")
                .concat(" FROM ")
                .concat(TRIPS_TABLE)
                .concat(" GROUP BY ")
                .concat(VENDOR_ID_COLUMN)
                .concat(" ORDER BY ")
                .concat("countedUnknownTrips");
    }
}
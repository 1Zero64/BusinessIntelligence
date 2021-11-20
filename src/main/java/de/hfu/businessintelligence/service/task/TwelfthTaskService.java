package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.FRACTION_COEFFICIENT;
import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;
import static de.hfu.businessintelligence.configuration.TableConfiguration.*;

public class TwelfthTaskService implements TaskService {

    private volatile static TwelfthTaskService instance;

    private final SparkSession sparkSession;

    private TwelfthTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static TwelfthTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (TwelfthTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new TwelfthTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        if (USE_CSV_OUTPUT) {
            FileService.getInstance().saveAsCsvFile(getTripsWithUnknownPaymentNoChargeOrDispute(), "tripsWithUnknownPaymentNoChargeOrDispute");
        } else {
            getTripsWithUnknownPaymentNoChargeOrDispute().write().mode(SaveMode.Overwrite).saveAsTable("tripsWithUnknownPaymentNoChargeOrDispute");
        }
    }

    private Dataset<Row> getTripsWithUnknownPaymentNoChargeOrDispute() {
        String statement = buildSqlStatement();
        return sparkSession.sql(statement)
                .sample(FRACTION_COEFFICIENT);
    }

    private String buildSqlStatement() {
        return "SELECT "
                .concat(PAYMENT_TYPE_COLUMN)
                .concat(", ")
                .concat(PICK_UP_LONGITUDE_COLUMN)
                .concat(", ")
                .concat(PICK_UP_LATITUDE_COLUMN)
                .concat(", ")
                .concat(DROP_OFF_LONGITUDE_COLUMN)
                .concat(", ")
                .concat(DROP_OFF_LATITUDE_COLUMN)
                .concat(" FROM ")
                .concat(TRIPS_TABLE)
                .concat(" WHERE ")
                .concat(PAYMENT_TYPE_COLUMN)
                .concat(" = 'UNK' OR ")
                .concat(PAYMENT_TYPE_COLUMN)
                .concat(" = 'NOC' OR ")
                .concat(PAYMENT_TYPE_COLUMN)
                .concat(" = 'DIS'");
    }

}

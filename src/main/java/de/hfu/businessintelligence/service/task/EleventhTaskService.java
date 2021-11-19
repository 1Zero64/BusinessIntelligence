package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;
import static de.hfu.businessintelligence.configuration.TableConfiguration.*;

public class EleventhTaskService implements TaskService {

    private volatile static EleventhTaskService instance;

    private final SparkSession sparkSession;

    private EleventhTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static EleventhTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (EleventhTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new EleventhTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        if (USE_CSV_OUTPUT) {
            FileService.getInstance().saveAsCsvFile(getCountedTripsAndTotalAmountPerPaymentType(), "countedTypeAndTotalAmountPerPaymentType");
        } else {
            getCountedTripsAndTotalAmountPerPaymentType().write().mode(SaveMode.Overwrite).saveAsTable("countedTypeAndTotalAmountPerPaymentType");
        }
    }

    private Dataset<Row> getCountedTripsAndTotalAmountPerPaymentType() {
        String statement = buildStatement();
        return sparkSession.sql(statement);
    }

    private String buildStatement() {
        return ("SELECT COUNT(*) as paymentTypeCount, SUM(")
                .concat(TOTAL_AMOUNT_COLUMN)
                .concat(") as totalAmount")
                .concat(" FROM ")
                .concat(TRIPS_TABLE)
                .concat(" GROUP BY ")
                .concat(PAYMENT_TYPE_COLUMN);
    }
}
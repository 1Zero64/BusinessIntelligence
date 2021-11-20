package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;
import static de.hfu.businessintelligence.configuration.TableConfiguration.*;

public class ThirteenthTaskService implements TaskService {

    private volatile static ThirteenthTaskService instance;

    private final SparkSession sparkSession;

    private ThirteenthTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static ThirteenthTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (ThirteenthTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new ThirteenthTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        if (USE_CSV_OUTPUT) {
            FileService.getInstance().saveAsCsvFile(getAvgTipAmountGroupByPaymentType(), "avgTipAmountInDollarsGroupByPaymentType");
            FileService.getInstance().saveAsCsvFile(getAvgTotalAmountGroupByPaymentType(), "avgTotalAmountInDollarsGroupByPaymentType");
        } else {
            getAvgTipAmountGroupByPaymentType().write().mode(SaveMode.Overwrite).saveAsTable("avgTipAmountInDollarsInDollarsGroupByPaymentType");
            getAvgTotalAmountGroupByPaymentType().write().mode(SaveMode.Overwrite).saveAsTable("avgTotalAmountInDollarsGroupByPaymentType");
        }
    }

    private Dataset<Row> getAvgTipAmountGroupByPaymentType() {
        String statement = buildAvgTipAmountPerPaymentTypeStatement();
        return sparkSession.sql(statement);
    }

    private Dataset<Row> getAvgTotalAmountGroupByPaymentType() {
        String statement = buildAvgTotalAmountPerPaymentTypeStatement();
        return sparkSession.sql(statement);
    }

    private String buildAvgTipAmountPerPaymentTypeStatement() {
        return "SELECT "
                .concat(PAYMENT_TYPE_COLUMN)
                .concat(", AVG(")
                .concat(TIP_AMOUNT_COLUMN)
                .concat(") as avgTipAmountInDollars FROM ")
                .concat(TRIPS_TABLE)
                .concat(" GROUP BY ")
                .concat(PAYMENT_TYPE_COLUMN);
    }

    private String buildAvgTotalAmountPerPaymentTypeStatement() {
        return "SELECT "
                .concat(PAYMENT_TYPE_COLUMN)
                .concat(", AVG(")
                .concat(TOTAL_AMOUNT_COLUMN)
                .concat(") as avgTotalAmountInDollars FROM ")
                .concat(TRIPS_TABLE)
                .concat(" GROUP BY ")
                .concat(PAYMENT_TYPE_COLUMN);
    }
}

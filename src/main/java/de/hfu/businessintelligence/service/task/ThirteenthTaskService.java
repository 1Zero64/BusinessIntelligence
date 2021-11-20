package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;

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
            FileService.getInstance().saveAsCsvFile(getAvgTipAmountGroupByPaymentType(), "getAvgTipAmountInDollarsGroupByPaymentType");
            FileService.getInstance().saveAsCsvFile(getAvgTotalAmountGroupByPaymentType(), "getAvgTotalAmountInDollarsGroupByPaymentType");
        } else {
            getAvgTipAmountGroupByPaymentType().write().mode(SaveMode.Overwrite).saveAsTable("getAvgTipAmountInDollarsInDollarsGroupByPaymentType");
            getAvgTotalAmountGroupByPaymentType().write().mode(SaveMode.Overwrite).saveAsTable("getAvgTotalAmountInDollarsGroupByPaymentType");
        }
    }

    private Dataset<Row> getAvgTipAmountGroupByPaymentType() {
        return sparkSession.sql("select paymentType, avg(tipAmount) as avgTipAmountInDollars from trips group by paymentType");
    }

    private Dataset<Row> getAvgTotalAmountGroupByPaymentType() {
        return sparkSession.sql("select paymentType, avg(totalAmount) as avgTotalAmountInDollars from trips group by paymentType");
    }
}

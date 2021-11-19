package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;

public class EighteenthTaskService implements TaskService{

    private volatile static EighteenthTaskService instance;

    private final SparkSession sparkSession;

    private EighteenthTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static EighteenthTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (EighteenthTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new EighteenthTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    public void executeTask() {
        if (USE_CSV_OUTPUT) {
            FileService.getInstance().saveAsCsvFile(getAvgPerPersonTotalAmountGroupByPassengerCount(), "getAvgTotalAmountInDollarsPerPersonGroupByPassengerCount");
            FileService.getInstance().saveAsCsvFile(getAvgPerPersonTotalAmount(), "getAvgTotalAmountInDollarsGroupByPaymentType");
        } else {
            getAvgPerPersonTotalAmountGroupByPassengerCount().write().mode(SaveMode.Overwrite).saveAsTable("getAvgPerPersonInDollarsTotalAmountGroupByPassengerCount");
            getAvgPerPersonTotalAmount().write().mode(SaveMode.Overwrite).saveAsTable("getAvgTotalAmountInDollarsGroupByPaymentType");
        }
    }

    private Dataset<Row> getAvgPerPersonTotalAmountGroupByPassengerCount() {
        return sparkSession.sql("Select avg(totalAmount) as AvgPerPersonTotalAmountInDollarsGroupByPassengerCount, passengerCount from trips group by passangerCount");
    }

    private Dataset<Row> getAvgPerPersonTotalAmount() {
        return sparkSession.sql("Select avg(totalAmount) as AvgPerPersonTotalAmountInDollars, passengerCount from trips");
    }
}

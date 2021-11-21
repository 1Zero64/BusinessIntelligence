package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;
import static de.hfu.businessintelligence.configuration.TableConfiguration.*;

public class EighteenthTaskService implements TaskService {

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

    @Override
    public void executeTask() {
        if (USE_CSV_OUTPUT) {
            FileService.getInstance().saveAsCsvFile(getAvgTotalAmountPerPersonGroupedByPassengerCount(), "avgTotalAmountInDollarPerPersonGroupedByPassengerCount");
        } else {
            getAvgTotalAmountPerPersonGroupedByPassengerCount().write().mode(SaveMode.Overwrite).saveAsTable("avgTotalAmountInDollarPerPersonGroupedByPassengerCount");
        }
    }

    private Dataset<Row> getAvgTotalAmountPerPersonGroupedByPassengerCount() {
        String statement = buildSqlStatement();
        return sparkSession.sql(statement);
    }

    private String buildSqlStatement() {
        return "SELECT "
                .concat(PASSENGER_COUNT_COLUMN)
                .concat(", AVG(")
                .concat(TOTAL_AMOUNT_COLUMN)
                .concat(" / ")
                .concat(PASSENGER_COUNT_COLUMN)
                .concat(") as avgTotalAmountInDollarsPerPerson FROM ")
                .concat(TRIPS_TABLE)
                .concat(" WHERE ")
                .concat(PASSENGER_COUNT_COLUMN)
                .concat(" BETWEEN 1 AND 9")
                .concat(" GROUP BY ")
                .concat(PASSENGER_COUNT_COLUMN);
    }
}

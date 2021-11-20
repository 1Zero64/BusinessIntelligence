package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;
import static de.hfu.businessintelligence.configuration.TableConfiguration.*;

public class FourteenthTaskService implements TaskService {

    private volatile static FourteenthTaskService instance;

    private final SparkSession sparkSession;

    private FourteenthTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static FourteenthTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (FourteenthTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new FourteenthTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    public void executeTask() {
        if (USE_CSV_OUTPUT) {
            FileService.getInstance().saveAsCsvFile(getCountedUnknownTripsPerFlag(), "countedUnknownTripsPerFlag");
        } else {
            getCountedUnknownTripsPerFlag().write().mode(SaveMode.Overwrite).saveAsTable("countedUnknownTripsPerFlag");
        }
    }

    private Dataset<Row> getCountedUnknownTripsPerFlag() {
        String statement = buildStatement();
        return sparkSession.sql(statement);
    }

    private String buildStatement() {
        // SELECT store_fwd_flag as flag, COUNT(*) as countedUnknownTrips FROM trips WHERE paymentType = 'UNK' GROUP BY flag;
        return "SELECT "
                .concat(STORE_AND_FWD_FLAG_COLUMN)
                .concat(" as flag, COUNT(*) as countedUnknownTrips")
                .concat(" FROM ")
                .concat(TRIPS_TABLE)
                .concat(" WHERE ")
                .concat(PAYMENT_TYPE_COLUMN)
                .concat(" = 'UNK'")
                .concat(" GROUP BY ")
                .concat("flag");
    }
}

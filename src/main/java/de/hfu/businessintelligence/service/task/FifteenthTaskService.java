package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;
import static de.hfu.businessintelligence.configuration.TableConfiguration.*;

public class FifteenthTaskService implements TaskService {

    private volatile static FifteenthTaskService instance;

    private final SparkSession sparkSession;

    private FifteenthTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static FifteenthTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (FifteenthTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new FifteenthTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    public void executeTask() {
        if (USE_CSV_OUTPUT) {
            FileService.getInstance().saveAsCsvFile(getCoordinatesWhereNoConnection(), "coordinatesWhereNoConnection");
        } else {
            getCoordinatesWhereNoConnection().write().mode(SaveMode.Overwrite).saveAsTable("coordinatesWhereNoConnection");
        }
    }

    private Dataset<Row> getCoordinatesWhereNoConnection() {
        String statement = buildStatement();
        return sparkSession.sql(statement);
    }

    private String buildStatement() {
        // SELECT dropOffLatitude, dropOffLongitude, paymentType, store_fwd_flag FROM trips WHERE store_fwd_flag = 'Y' AND paymentType = 'UNK';
        return "SELECT "
                .concat(DROP_OFF_LATITUDE_COLUMN)
                .concat(", ")
                .concat(DROP_OFF_LONGITUDE_COLUMN)
                .concat(", ")
                .concat(PAYMENT_TYPE_COLUMN)
                .concat(", ")
                .concat(STORE_AND_FWD_FLAG_COLUMN)
                .concat(" FROM ")
                .concat(TRIPS_TABLE)
                .concat(" WHERE ")
                .concat(STORE_AND_FWD_FLAG_COLUMN)
                .concat(" = 'Y'")
                .concat(" AND ")
                .concat(PAYMENT_TYPE_COLUMN)
                .concat(" = 'UNK'");
    }
}
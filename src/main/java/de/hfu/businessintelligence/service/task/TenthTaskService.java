package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;
import static de.hfu.businessintelligence.configuration.TableConfiguration.*;

public class TenthTaskService implements TaskService {

    private volatile static TenthTaskService instance;

    private final SparkSession sparkSession;

    private TenthTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static TenthTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (TenthTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new TenthTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        if (USE_CSV_OUTPUT) {
            FileService.getInstance().saveAsCsvFile(getTenMostVisitedPlaces(), "tenMostVisitedPlaces");
        } else {
            getTenMostVisitedPlaces().write().mode(SaveMode.Overwrite).saveAsTable("tenMostVisitedPlaces");
        }
    }

    private Dataset<Row> getTenMostVisitedPlaces() {
        String statement = buildSqlStatement();
        return sparkSession.sql(statement);
    }

    private String buildSqlStatement() {
        return "SELECT ROUND("
                .concat(DROP_OFF_LATITUDE_COLUMN)
                .concat(", 4) as ")
                .concat(DROP_OFF_LATITUDE_COLUMN)
                .concat(", ROUND(")
                .concat(DROP_OFF_LONGITUDE_COLUMN)
                .concat(", 4) as ")
                .concat(DROP_OFF_LONGITUDE_COLUMN)
                .concat(", COUNT(*) as tripsCount FROM ")
                .concat(TRIPS_TABLE)
                .concat(" GROUP BY ")
                .concat(DROP_OFF_LATITUDE_COLUMN)
                .concat(", ")
                .concat(DROP_OFF_LONGITUDE_COLUMN)
                .concat(" HAVING ")
                .concat(DROP_OFF_LATITUDE_COLUMN)
                .concat(" <> 0 AND ")
                .concat(DROP_OFF_LONGITUDE_COLUMN)
                .concat(" <> 0 ORDER BY tripsCount DESC LIMIT 10");
    }
}

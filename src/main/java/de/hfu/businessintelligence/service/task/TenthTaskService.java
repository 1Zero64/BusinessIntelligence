package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.configuration.CsvConfiguration;
import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;

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
        return sparkSession.sql("SELECT ROUND(dropOffLatitude, 4) as dropOffLatitudeRounded, ROUND(dropOffLongitude, 4) as dropOffLongitudeRounded, COUNT(*) as tripsCount FROM trips GROUP BY ROUND(dropOffLatitude, 4), ROUND(dropOffLongitude, 4) ORDER BY tripsCount DESC LIMIT 10");

        /**
         *   10 meiste besuchte Orte -> Koordinaten runden
         *   "SELECT ROUND("
         *           .concat(dropOffLatitude)
         *           .concat(", 4 as ")
         *           .concat(dropOffLatitude)
         *           .concat(, ROUND(")
         *           .concat(dropOffLongitude)
         *           .concat(", 4) as ")
         *           .concat(pickupLongitude)
         *           .concat(, COUNT(*) as tripsCount")
         *           .concat(" FROM ")
         *           .concat(TRIPS_TABLE)
         *           .concat(" GROUP BY ");
         *           .concat(pickupLatitude)
         *           .concat(", ")
         *           .concat(pickupLongitude)
         *           .concat(" ORDER BY ")
         *           .concat("tripsCount DESC")
         *           .concat(" LIMIT 10");
         */
    }
}

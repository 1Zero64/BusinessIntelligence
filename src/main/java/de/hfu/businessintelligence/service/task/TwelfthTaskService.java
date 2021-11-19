package de.hfu.businessintelligence.service.task;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

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

    }

    private Dataset<Row> getUnknownAndUnvoidedTrips() {
        return sparkSession.sql("select pickupLongitude, pickupLatitude, dropOffLongitude, dropOffLatitude from trips where paymentType = 'UNK' or paymentType")
    }



}

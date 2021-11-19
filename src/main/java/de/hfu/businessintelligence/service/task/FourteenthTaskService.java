package de.hfu.businessintelligence.service.task;

import org.apache.spark.sql.SparkSession;

import java.util.Optional;

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

    }

    private Dataset<Row> getCountedTripsPerFlag {
        return sparkSession.sql("SELECT str_fwd_flag as flag, count(*) as countedTrips FROM trips GROUP BY str_fwd_flag");
    }

}
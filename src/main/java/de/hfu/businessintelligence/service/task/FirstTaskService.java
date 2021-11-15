package de.hfu.businessintelligence.service.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

@Slf4j
public class FirstTaskService implements TaskService {

    private static final double KILOMETERS_TO_MILES = 0.621371d;

    private volatile static FirstTaskService instance;

    private final SparkSession sparkSession;

    private FirstTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static FirstTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (FirstTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new FirstTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        getAverageIncomeBetween(0, Double.MAX_VALUE).write().mode(SaveMode.Overwrite).saveAsTable("averageIncome");
        getAverageIncomeBetween(0, 30).write().mode(SaveMode.Overwrite).saveAsTable("averageIncomeBetween0And30Kilometers");
        getAverageIncomeBetween(30, 50).write().mode(SaveMode.Overwrite).saveAsTable("averageIncomeBetween30And50Kilometers");
    }

    public Dataset<Row> getAverageIncomeBetween(double minTripDistanceInKilometers, double maxTripDistanceInKilometers) {
        String statement = buildSqlStatementWith(minTripDistanceInKilometers, maxTripDistanceInKilometers);
        return sparkSession.sql(statement);
    }

    private String buildSqlStatementWith(double minTripDistanceInKilometers, double maxTripDistanceInKilometers) {
        double minTripDistanceInMiles = minTripDistanceInKilometers * KILOMETERS_TO_MILES;
        double maxTripDistanceInMiles = maxTripDistanceInKilometers * KILOMETERS_TO_MILES;

        return "select AVG(totalAmount) as avgTotalAmount FROM trips WHERE tripDistance > "
                .concat(String.valueOf(minTripDistanceInMiles))
                .concat(" AND tripDistance < ")
                .concat(String.valueOf(maxTripDistanceInMiles));
    }
}

package de.hfu.businessintelligence.service.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
        getAverageIncomeBetween(0, Double.MAX_VALUE, "averageIncomeAndAverageDistance").show();
//        getAverageIncomeBetween(0, 30, "averageIncomeBetweenZeroAndThirtyKilometers").show();
//        getAverageIncomeBetween(30, 50, "averageIncomeBetweenThirtyAndFiftyKilometers").show();
    }

    public Dataset<Row> getAverageIncomeBetween(double minTripDistanceInKilometers, double maxTripDistanceInKilometers, String name) {
        String statement = buildSqlStatementWith(minTripDistanceInKilometers, maxTripDistanceInKilometers, name);
        return sparkSession.sql(statement);
    }

    private String buildSqlStatementWith(double minTripDistanceInKilometers, double maxTripDistanceInKilometers, String name) {
        double minTripDistanceInMiles = minTripDistanceInKilometers * KILOMETERS_TO_MILES;
        double maxTripDistanceInMiles = maxTripDistanceInKilometers * KILOMETERS_TO_MILES;

        return "select AVG(totalAmount), AVG(tripDistance) * 1.60934 as "
                .concat(name)
                .concat(" FROM trips WHERE tripDistance > ")
                .concat(String.valueOf(minTripDistanceInMiles))
                .concat(" AND tripDistance < ")
                .concat(String.valueOf(maxTripDistanceInMiles));
    }
}

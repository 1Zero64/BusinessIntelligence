package de.hfu.businessintelligence.service.task;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.TableConfiguration.*;
import static de.hfu.businessintelligence.configuration.UnitConfiguration.MILES_TO_KILOMETERS;
import static de.hfu.businessintelligence.configuration.UnitConfiguration.SECONDS_TO_HOURS;

public class ThirdTaskService implements TaskService {

    private volatile static ThirdTaskService instance;

    private final SparkSession sparkSession;

    private ThirdTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static ThirdTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (ThirdTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new ThirdTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        getAverageVelocity().write().mode(SaveMode.Overwrite).saveAsTable("averageVelocityInKilometersPerHour");
    }

    private Dataset<Row> getAverageVelocity() {
        String statement = buildSqlStatement();
        return sparkSession.sql(statement);
    }

    private String buildSqlStatement() {
        return "SELECT AVG(("
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(" * ")
                .concat(String.valueOf(MILES_TO_KILOMETERS))
                .concat(") / (")
                .concat(TRIP_TIME_IN_SECONDS_COLUMN)
                .concat(" / ")
                .concat(String.valueOf(SECONDS_TO_HOURS))
                .concat(")) as avgVelocityInKilometersPerHour FROM ")
                .concat(TRIPS_TABLE);
    }
}

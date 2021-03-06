package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;
import static de.hfu.businessintelligence.configuration.TableConfiguration.*;
import static de.hfu.businessintelligence.configuration.UnitConfiguration.MILES_TO_KILOMETERS;
import static de.hfu.businessintelligence.configuration.UnitConfiguration.SECONDS_TO_HOURS;

public class FourthTaskService implements TaskService, Serializable {

    private static final String HOURS_COLUMN = "hour";
    private static final String EXTRACT_HOUR_USER_DEFINED_FUNCTION = "EXTRACT_HOUR";

    private volatile static FourthTaskService instance;

    private final SparkSession sparkSession;

    private FourthTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static FourthTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (FourthTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new FourthTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        if (USE_CSV_OUTPUT) {
            FileService.getInstance().saveAsCsvFile(getAvgVelocityGroupedByDayTimes(), "averageVelocityInKilometersPerHourGroupedByHour");
        } else {
            getAvgVelocityGroupedByDayTimes().write().mode(SaveMode.Overwrite).saveAsTable("averageVelocityInKilometersPerHourGroupedByHour");
        }
    }

    private Dataset<Row> getAvgVelocityGroupedByDayTimes() {
        sparkSession.udf().register(
                EXTRACT_HOUR_USER_DEFINED_FUNCTION,
                (UDF1<Timestamp, Integer>) this::extractHourFromDateTime,
                DataTypes.IntegerType
        );
        String statement = buildSqlStatement();
        return sparkSession.sql(statement)
                .sort(functions.asc(HOURS_COLUMN));
    }

    private String buildSqlStatement() {
        return "SELECT "
                .concat(EXTRACT_HOUR_USER_DEFINED_FUNCTION)
                .concat("(")
                .concat(PICKUP_DATE_TIME_COLUMN)
                .concat(")")
                .concat(" as ")
                .concat(HOURS_COLUMN)
                .concat(", AVG((")
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(" * ")
                .concat(String.valueOf(MILES_TO_KILOMETERS))
                .concat(") / (")
                .concat(TRIP_TIME_IN_SECONDS_COLUMN)
                .concat(" / ")
                .concat(String.valueOf(SECONDS_TO_HOURS))
                .concat(")) as avgVelocityInKilometersPerHour FROM ")
                .concat(TRIPS_TABLE)
                .concat(" WHERE ((")
                .concat(TRIP_DISTANCE_COLUMN)
                .concat(" * ")
                .concat(String.valueOf(MILES_TO_KILOMETERS))
                .concat(") / (")
                .concat(TRIP_TIME_IN_SECONDS_COLUMN)
                .concat(" / ")
                .concat(String.valueOf(SECONDS_TO_HOURS))
                .concat(")) BETWEEN 0 AND 80")
                .concat(" GROUP BY ")
                .concat(EXTRACT_HOUR_USER_DEFINED_FUNCTION)
                .concat("(")
                .concat(PICKUP_DATE_TIME_COLUMN)
                .concat(")");
    }

    private Integer extractHourFromDateTime(Timestamp timestamp) {
        return timestamp.toLocalDateTime().getHour();
    }
}

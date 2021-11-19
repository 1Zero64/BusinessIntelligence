package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;
import static de.hfu.businessintelligence.configuration.TableConfiguration.PICKUP_DATE_TIME_COLUMN;
import static de.hfu.businessintelligence.configuration.TableConfiguration.TRIPS_TABLE;

public class FifthTaskService implements TaskService, Serializable {

    private static final String HOUR_COLUMN = "hour";
    private static final String EXTRACT_HOUR_USER_DEFINED_FUNCTION = "EXTRACT_HOUR";
    private static final String EXTRACT_DATE_TIME_WITH_HOUR_USER_DEFINED_FUNCTION = "EXTRACT_DATE_TIME_WITH_HOUR";

    private volatile static FifthTaskService instance;

    private final SparkSession sparkSession;

    private FifthTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static FifthTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (FifthTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new FifthTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        if (USE_CSV_OUTPUT) {
            FileService.getInstance().saveAsCsvFile(avgTripsGroupedByHours(), "avgCountedTripsGroupedByHours");
        } else {
            avgTripsGroupedByHours().write().mode(SaveMode.Overwrite).saveAsTable("avgCountedTripsGroupedByHours");
        }
    }

    private Dataset<Row> avgTripsGroupedByHours() {
        sparkSession.udf().register(
                EXTRACT_HOUR_USER_DEFINED_FUNCTION,
                (UDF1<Timestamp, Integer>) this::extractHourFromDateTime,
                DataTypes.IntegerType
        );
        sparkSession.udf().register(
                EXTRACT_DATE_TIME_WITH_HOUR_USER_DEFINED_FUNCTION,
                (UDF1<Timestamp, Timestamp>) this::extractDateTimeWithOnlyHoursFromDateTime,
                DataTypes.TimestampType
        );
        String statement = buildSqlStatement();
        return sparkSession.sql(statement)
                .sort(functions.asc(HOUR_COLUMN));
    }

    private String buildSqlStatement() {
        return "WITH newTrips as (SELECT "
                .concat(EXTRACT_DATE_TIME_WITH_HOUR_USER_DEFINED_FUNCTION)
                .concat("(")
                .concat(PICKUP_DATE_TIME_COLUMN)
                .concat(") as dateTime, COUNT(*) as countedDrives FROM ")
                .concat(TRIPS_TABLE)
                .concat(" GROUP BY ")
                .concat(EXTRACT_DATE_TIME_WITH_HOUR_USER_DEFINED_FUNCTION)
                .concat("(")
                .concat(PICKUP_DATE_TIME_COLUMN)
                .concat(")) SELECT ")
                .concat(EXTRACT_HOUR_USER_DEFINED_FUNCTION)
                .concat("(dateTime) as ")
                .concat(HOUR_COLUMN)
                .concat(", AVG(countedDrives) as avgCountedDrives FROM newTrips GROUP BY ")
                .concat(EXTRACT_HOUR_USER_DEFINED_FUNCTION)
                .concat("(dateTime)");
    }

    private Integer extractHourFromDateTime(Timestamp timestamp) {
        return timestamp.toLocalDateTime().getHour();
    }

    private Timestamp extractDateTimeWithOnlyHoursFromDateTime(Timestamp timestamp) {
        LocalDateTime dateTime = timestamp.toLocalDateTime();
        return Timestamp.valueOf(dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH")).concat(":00:00"));
    }
}

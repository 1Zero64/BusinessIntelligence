package de.hfu.businessintelligence.service.task;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class FifthTaskService implements TaskService, Serializable {

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
        avgTripsGroupedByHours().write().mode(SaveMode.Overwrite).saveAsTable("avgCountedTripsGroupedByHours");
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
        return sparkSession.sql("WITH newTrips as (SELECT EXTRACT_DATE_TIME_WITH_HOUR(pickUpDateTime) as dateTime, COUNT(*) as countedDrives FROM trips GROUP BY EXTRACT_DATE_TIME_WITH_HOUR(pickUpDateTime)) SELECT EXTRACT_HOUR(dateTime) AS hour, AVG(countedDrives) AS avgCountedDrives FROM newTrips GROUP BY EXTRACT_HOUR(dateTime)")
                .sort(functions.asc("hour"))
    }

    // WITH help as (SELECT age FROM persons)
    // SELECT * FROM help

    private String buildSqlStatementForCountingTripsGroupedByDateTime() {
        return null;
    }

    private Integer extractHourFromDateTime(Timestamp timestamp) {
        return timestamp.toLocalDateTime().getHour();
    }

    private Timestamp extractDateTimeWithOnlyHoursFromDateTime(Timestamp timestamp) {
        LocalDateTime dateTime = timestamp.toLocalDateTime();
        return Timestamp.valueOf(dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH")).concat(":00:00"));
    }
}

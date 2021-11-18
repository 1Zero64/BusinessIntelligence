package de.hfu.businessintelligence.service.task;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Optional;

import static de.hfu.businessintelligence.configuration.TableConfiguration.*;

public class SixthTaskService implements TaskService, Serializable {

    private static final String DROP_OFF_TIME_COLUMN = "dropOffTime";
    private static final String EXTRACT_HOUR_USER_DEFINED_FUNCTION = "EXTRACT_HOUR";

    private volatile static SixthTaskService instance;

    private final SparkSession sparkSession;

    private SixthTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static SixthTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (SixthTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new SixthTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        getAvgTipAmountsGroupedByTime().write().mode(SaveMode.Overwrite).saveAsTable("avgTipAmountInDollarsGroupedByDropOffTime");
    }

    private Dataset<Row> getAvgTipAmountsGroupedByTime() {
        sparkSession.udf().register(
                EXTRACT_HOUR_USER_DEFINED_FUNCTION,
                (UDF1<Timestamp, Integer>) this::extractHourFromDateTime,
                DataTypes.IntegerType
        );
        String statement = buildSqlStatement();
        return sparkSession.sql(statement)
                .sort(functions.asc(DROP_OFF_TIME_COLUMN));
    }

    private String buildSqlStatement() {
        return "SELECT "
                .concat(EXTRACT_HOUR_USER_DEFINED_FUNCTION)
                .concat("(")
                .concat(DROP_OFF_DATE_TIME_COLUMN)
                .concat(")")
                .concat(" as ")
                .concat(DROP_OFF_TIME_COLUMN)
                .concat(", AVG(")
                .concat(TIP_AMOUNT_COLUMN)
                .concat(") as avgTipAmountInDollars FROM ")
                .concat(TRIPS_TABLE)
                .concat(" GROUP BY ")
                .concat(EXTRACT_HOUR_USER_DEFINED_FUNCTION)
                .concat("(")
                .concat(DROP_OFF_DATE_TIME_COLUMN)
                .concat(")");
    }

    private Integer extractHourFromDateTime(Timestamp timestamp) {
        return timestamp.toLocalDateTime().getHour();
    }
}

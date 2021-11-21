package de.hfu.businessintelligence.service.task;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.*;
import scala.collection.immutable.IntMap;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.USE_CSV_OUTPUT;
import static de.hfu.businessintelligence.configuration.TableConfiguration.*;

public class EighthTaskService implements TaskService {

    private volatile static EighthTaskService instance;

    private final SparkSession sparkSession;

    private EighthTaskService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static EighthTaskService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (EighthTaskService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new EighthTaskService(sparkSession);
                }
            }
        }
        return instance;
    }

    @Override
    public void executeTask() {
        if (USE_CSV_OUTPUT) {
            FileService.getInstance().saveAsCsvFile(getAvgTipAmountGroupedByTripTime(), "avgTipAmountInDollarsGroupedByTripTimeInMinutes");
        } else {
            getAvgTipAmountGroupedByTripTime().write().mode(SaveMode.Overwrite).saveAsTable("avgTipAmountInDollarsGroupedByTripTimeInMinutes");
        }
    }

    private Dataset<Row> getAvgTipAmountGroupedByTripTime() {
        String statement = buildSqlStatement();
        return sparkSession.sql(statement)
                .sort(functions.asc("minutes"));
    }

    private String buildSqlStatement() {
        return "WITH newTrips as (SELECT CEIL(("
                .concat(TRIP_TIME_IN_SECONDS_COLUMN)
                .concat(") / 150) as minuteIntervall, ")
                .concat(TIP_AMOUNT_COLUMN)
                .concat(" FROM ")
                .concat(TRIPS_TABLE)
                .concat(" WHERE ")
                .concat(TRIP_TIME_IN_SECONDS_COLUMN)
                .concat(" BETWEEN 1 AND 4200)")
                .concat(" SELECT ")
                .concat("(minuteIntervall * 2.5) as minutes, AVG(")
                .concat(TIP_AMOUNT_COLUMN)
                .concat(") as avgTipAmount")
                .concat(" FROM ")
                .concat("newTrips")
                .concat(" GROUP BY ")
                .concat("minutes");
    }

    // WITH newTrips as (SELECT CEIL((tripTimeInSeconds) / 150) as minuteIntervall, tipAmount FROM trips WHERE tripTimeInSeconds BETWEEN 1 AND 4200)
    // SELECT (minuteIntervall * 2.5) as minutes, AVG(tipAmount) as avgTipAmount FROM newTrips GROUP BY minutes
}

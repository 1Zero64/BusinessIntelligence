package de.hfu.businessintelligence.service.mapper;

import de.hfu.businessintelligence.service.support.FileService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.List;
import java.util.Optional;

import static de.hfu.businessintelligence.configuration.TableConfiguration.*;

public class TripMapperService {

    private static final String TRIP_FARE_DIRECTORY = "/home/hennihaus/businessintelligence/trip_fare_test";
    private static final String TRIP_DATA_DIRECTORY = "/home/hennihaus/businessintelligence/trip_data_test";

    private volatile static TripMapperService instance;

    private final SparkSession sparkSession;

    private TripMapperService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static TripMapperService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (TripMapperService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new TripMapperService(sparkSession);
                }
            }
        }
        return instance;
    }

    public Dataset<Row> getAllTrips() {
        // read all trip fares into data frame
        String[] tripFareFiles = FileService.getInstance().getAllFilePathsFrom(TRIP_FARE_DIRECTORY);
        Dataset<Row> tripFares = TripFareMapperService.getInstance(sparkSession).mapToTripFares(true, tripFareFiles);
        // read all trip data into data frame
        String[] tripDataFiles = FileService.getInstance().getAllFilePathsFrom(TRIP_DATA_DIRECTORY);
        Dataset<Row> tripData = TripDataMapperService.getInstance(sparkSession).mapToTripData(true, tripDataFiles);

        Dataset<Row> trips = tripData.join(tripFares, getKeyColumns());
        trips.createOrReplaceTempView(TRIPS_TABLE);
        trips.write().mode(SaveMode.Overwrite).saveAsTable(TRIPS_TABLE);
        return trips;
    }

    private Seq<String> getKeyColumns() {
        return JavaConversions.asScalaBuffer(List.of(
                MEDALLION_COLUMN,
                HACK_LICENSE_COLUMN,
                VENDOR_ID_COLUMN,
                PICKUP_DATE_TIME_COLUMN
        ));
    }
}

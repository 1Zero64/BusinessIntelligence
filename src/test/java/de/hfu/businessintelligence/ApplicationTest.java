package de.hfu.businessintelligence;

import de.hfu.businessintelligence.configuration.SparkConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConversions;

import java.util.List;

import static de.hfu.businessintelligence.configuration.TableConfiguration.MEDALLION_COLUMN;

public class ApplicationTest {

    public static void main(String[] args) {
        SparkSession spark = SparkConfiguration.getInstance().sparkSession();
        Dataset<Row> tripData = spark.read().option("header", true).csv("C:\\businessintelligence\\trip_data_test\\trip_data_1.csv");
        Dataset<Row> tripFare = spark.read().option("header", true).csv("C:\\businessintelligence\\trip_fare_test\\trip_fare_1.csv");

        Dataset<Row> trips = tripData.join(tripFare, JavaConversions.asScalaBuffer(List.of(
                "medallion"
        )));

        Dataset<Row> sampledTrips = trips.sample(0.00001d);

        sampledTrips
                .select(MEDALLION_COLUMN, "hack_license", "vendor_id", "rate_code", "store_and_fwd_flag", "pickup_datetime", "dropoff_datetime", "passenger_count", "trip_time_in_secs", "trip_distance", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude")
                .repartition(2)
                .write()
                .option("header", true)
                .csv("C:\\test\\trip_data_test");

        sampledTrips
                .select(MEDALLION_COLUMN, "hack_license", "vendor_id", "pickup_datetime", " payment_type", " fare_amount", " surcharge", " mta_tax", " tip_amount", " tolls_amount", " total_amount")
                .repartition(2)
                .write()
                .option("header", true)
                .csv("C:\\test\\trip_fare_test");
    }
}

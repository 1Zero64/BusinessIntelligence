package de.hfu.businessintelligence.service.mapper;

import de.hfu.businessintelligence.model.TripData;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.*;

@Slf4j
public class TripDataMapperService implements Serializable {

    private static final int RATE_CODE_FIELD_INDEX = 3;
    private static final int STORE_AND_FWD_FLAG_FIELD_INDEX = 4;
    private static final int PICKUP_DATETIME_FIELD_INDEX = 5;
    private static final int DROP_OFF_DATETIME_FIELD_INDEX = 6;
    private static final int PASSENGER_COUNT_FIELD_INDEX = 7;
    private static final int TRIP_TIME_IN_SECS_FIELD_INDEX = 8;
    private static final int TRIP_DISTANCE_FIELD_INDEX = 9;
    private static final int PICKUP_LONGITUDE_FIELD_INDEX = 10;
    private static final int PICKUP_LATITUDE_FIELD_INDEX = 11;
    private static final int DROP_OFF_LONGITUDE_FIELD_INDEX = 12;
    private static final int DROP_OFF_LATITUDE_FIELD_INDEX = 13;
    private static final int TRIP_DATA_ROW_LENGTH = 14;

    private volatile static TripDataMapperService instance;

    private final SparkSession sparkSession;

    private TripDataMapperService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static TripDataMapperService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized ((TripDataMapperService.class)) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new TripDataMapperService(sparkSession);
                }
            }
        }
        return instance;
    }

    public Dataset<Row> mapToTripData(boolean hasHeader, String... filePaths) {
        JavaRDD<TripData> tripDataRdd = extractTripDataRddFrom(hasHeader, filePaths);
        return sparkSession.createDataFrame(tripDataRdd, TripData.class);
    }

    private JavaRDD<TripData> extractTripDataRddFrom(boolean hasHeader, String... filePaths) {
        JavaRDD<String> lines = extractLineRddFrom(filePaths);
        if (hasHeader) {
            String header = lines.first();
            return lines.filter(line -> !line.equals(header))
                    .map(line -> line.split(CSV_SEPARATOR))
                    .filter(this::filterFields)
                    .map(this::extractTripDataFromFields);
        }
        return lines.map(line -> line.split(CSV_SEPARATOR))
                .filter(this::filterFields)
                .map(this::extractTripDataFromFields);
    }

    private JavaRDD<String> extractLineRddFrom(String... filePaths) {
        return sparkSession.read()
                .textFile(filePaths)
                .javaRDD();
    }

    private boolean filterFields(String[] fields) {
        if (fields.length != TRIP_DATA_ROW_LENGTH) {
            log.warn("Row has just ".concat(String.valueOf(fields.length)).concat(" entries and is consequently sorted out!"));
            return false;
        }
        return true;
    }

    private TripData extractTripDataFromFields(String[] fields) {
        TripData tripData = new TripData();

        tripData.setMedallion(fields[MEDALLION_FIELD_INDEX].trim());
        tripData.setHackLicense(fields[HACK_LICENSE_FIELD_INDEX].trim());
        tripData.setVendorId(fields[VENDOR_ID_FIELD_INDEX]);
        tripData.setRateCode(Integer.parseInt(fields[RATE_CODE_FIELD_INDEX]));
        tripData.setStoreAndFwdFlag(fields[STORE_AND_FWD_FLAG_FIELD_INDEX]);
        tripData.setPickupDateTime(extractDateTimeFrom(fields[PICKUP_DATETIME_FIELD_INDEX]));
        tripData.setDropOffDateTime(extractDateTimeFrom(fields[DROP_OFF_DATETIME_FIELD_INDEX]));
        tripData.setPassengerCount(Integer.parseInt(fields[PASSENGER_COUNT_FIELD_INDEX]));
        tripData.setTripTimeInSeconds(Integer.parseInt(fields[TRIP_TIME_IN_SECS_FIELD_INDEX]));
        tripData.setTripDistance(Double.parseDouble(fields[TRIP_DISTANCE_FIELD_INDEX]));
        tripData.setPickupLongitude(Double.parseDouble(fields[PICKUP_LONGITUDE_FIELD_INDEX]));
        tripData.setPickupLatitude(Double.parseDouble(fields[PICKUP_LATITUDE_FIELD_INDEX]));
        tripData.setDropOffLongitude(Double.parseDouble(fields[DROP_OFF_LONGITUDE_FIELD_INDEX]));
        tripData.setDropOffLatitude(Double.parseDouble(fields[DROP_OFF_LATITUDE_FIELD_INDEX]));

        return tripData;
    }

    private Timestamp extractDateTimeFrom(String field) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_TIME_PATTERN);
            Date parsedDate = dateFormat.parse(field);
            return new Timestamp(parsedDate.getTime());
        } catch (ParseException e) {
            log.error("Error while parsing datetime: ", e);
            return null;
        }
    }
}

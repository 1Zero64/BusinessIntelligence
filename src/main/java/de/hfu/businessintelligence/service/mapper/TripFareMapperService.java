package de.hfu.businessintelligence.service.mapper;

import de.hfu.businessintelligence.model.TripFare;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.*;

@Slf4j
public class TripFareMapperService implements Serializable {

    private static final int PICKUP_DATE_TIME_FIELD_INDEX = 3;
    private static final int PAYMENT_TYPE_FIELD_INDEX = 4;
    private static final int FARE_AMOUNT_FIELD_INDEX = 5;
    private static final int SURCHAGE_FIELD_INDEX = 6;
    private static final int MTA_TAX_FIELD_INDEX = 7;
    private static final int TIP_AMOUNT_FIELD_INDEX = 8;
    private static final int TOLLS_AMOUNT_FIELD_INDEX = 9;
    private static final int TOTAL_AMOUNT_FIELD_INDEX = 10;

    private volatile static TripFareMapperService instance;

    private final SparkSession sparkSession;

    private TripFareMapperService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static TripFareMapperService getInstance(SparkSession sparkSession) {
        if (Optional.ofNullable(instance).isEmpty()) {
            synchronized (TripFareMapperService.class) {
                if (Optional.ofNullable(instance).isEmpty()) {
                    instance = new TripFareMapperService(sparkSession);
                }
            }
        }
        return instance;
    }

    public Dataset<Row> mapToTripFares(boolean hasHeader, String... filePaths) {
        JavaRDD<TripFare> tripFaresRdd = extractTripFareRddFrom(hasHeader, filePaths);
        return sparkSession.createDataFrame(tripFaresRdd, TripFare.class);
    }

    private JavaRDD<TripFare> extractTripFareRddFrom(boolean hasHeader, String... filePaths) {
        JavaRDD<String> lines = extractLineRddFrom(filePaths);
        if (hasHeader) {
            String header = lines.first();
            return lines.filter(line -> !isHeader(header, line))
                    .map(this::extractTripFareFromLine);
        }
        return lines.map(this::extractTripFareFromLine);

    }

    private JavaRDD<String> extractLineRddFrom(String... filePaths) {
        return sparkSession.read()
                .textFile(filePaths)
                .javaRDD();
    }

    private boolean isHeader(String header, String line) {
        List<String> fields = Arrays.stream(line.split(CSV_SEPARATOR)).map(String::trim).collect(Collectors.toList());
        List<String> headers = Arrays.stream(header.split(CSV_SEPARATOR)).map(String::trim).collect(Collectors.toList());
        if (fields.containsAll(headers)) {
            log.warn("Line: ".concat(line).concat(" is header!"));
        }
        return fields.containsAll(headers);
    }

    private TripFare extractTripFareFromLine(String line) {
        String[] fields = line.split(CSV_SEPARATOR);
        return extractTripFareFromFields(fields);
    }

    private TripFare extractTripFareFromFields(String[] fields) {
        TripFare tripFare = new TripFare();

        tripFare.setMedallion(fields[MEDALLION_FIELD_INDEX]);
        tripFare.setHackLicense(fields[HACK_LICENSE_FIELD_INDEX]);
        tripFare.setVendorId(fields[VENDOR_ID_FIELD_INDEX]);
        tripFare.setPickUpDatetime(extractDateTimeFrom(fields[PICKUP_DATE_TIME_FIELD_INDEX]));
        tripFare.setPaymentType(fields[PAYMENT_TYPE_FIELD_INDEX]);
        tripFare.setFareAmount(Double.parseDouble(fields[FARE_AMOUNT_FIELD_INDEX]));
        tripFare.setSurchage(Double.parseDouble(fields[SURCHAGE_FIELD_INDEX]));
        tripFare.setMtaTax(Double.parseDouble(fields[MTA_TAX_FIELD_INDEX]));
        tripFare.setTipAmount(Double.parseDouble(fields[TIP_AMOUNT_FIELD_INDEX]));
        tripFare.setTollsAmount(Double.parseDouble(fields[TOLLS_AMOUNT_FIELD_INDEX]));
        tripFare.setTotalAmount(Double.parseDouble(fields[TOTAL_AMOUNT_FIELD_INDEX]));

        return tripFare;
    }

    private Timestamp extractDateTimeFrom(String field) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date parsedDate = dateFormat.parse(field);
            return new Timestamp(parsedDate.getTime());
        } catch (ParseException e) {
            log.error("Error while parsing datetime: ", e);
            return null;
        }
    }
}

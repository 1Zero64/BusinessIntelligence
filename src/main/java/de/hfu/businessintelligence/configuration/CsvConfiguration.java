package de.hfu.businessintelligence.configuration;

public class CsvConfiguration {

    public static final String RESULT_DIRECTORY = "/opt/spark/csv/";
    public static final String TRIP_FARE_DIRECTORY = "/home/hennihaus/businessintelligence/trip_fare";
    public static final String TRIP_DATA_DIRECTORY = "/home/hennihaus/businessintelligence/trip_data";
    public static final String CSV_SEPARATOR = ",";
    public static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public static final int MEDALLION_FIELD_INDEX = 0;
    public static final int HACK_LICENSE_FIELD_INDEX = 1;
    public static final int VENDOR_ID_FIELD_INDEX = 2;

    public static final boolean USE_CSV_OUTPUT = true;
    public static final boolean WITH_HEADER = true;
}

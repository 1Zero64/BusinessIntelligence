package de.hfu.businessintelligence.configuration;

public class TableConfiguration {

    public static final String TRIPS_TABLE = "trips";
    // trip data and trip fare columns
    public static final String MEDALLION_COLUMN = "medallion";
    public static final String HACK_LICENSE_COLUMN = "hackLicense";
    public static final String VENDOR_ID_COLUMN = "vendorId";
    public static final String DROP_OFF_DATE_TIME_COLUMN = "dropOffDateTime";
    // trip data columns
    public static final String RATE_CODE_COLUMN = "rateCode";
    public static final String STORE_AND_FWD_FLAG_COLUMN = "storeAndFwdFlag";
    public static final String PICKUP_DATE_TIME_COLUMN = "pickUpDatetime";
    public static final String PASSENGER_COUNT_COLUMN = "passengerCount";
    public static final String TRIP_TIME_IN_SECONDS_COLUMN = "tripTimeInSeconds";
    public static final String TRIP_DISTANCE_COLUMN = "tripDistance";
    public static final String PICK_UP_LONGITUDE_COLUMN = "pickupLongitude";
    public static final String PICK_UP_LATITUDE_COLUMN = "pickupLatitude";
    public static final String DROP_OFF_LONGITUDE_COLUMN = "dropOffLongitude";
    public static final String DROP_OFF_LATITUDE_COLUMN = "dropOffLatitude";
    // trip fare columns
    public static final String PAYMENT_TYPE_COLUMN = "paymentType";
    public static final String FARE_AMOUNT_COLUMN = "fareAmount";
    public static final String SURCHAGE_COLUMN = "surchage";
    public static final String MTA_TAX_COLUMN = "mtaTax";
    public static final String TIP_AMOUNT_COLUMN = "tipAmount";
    public static final String TOLLS_AMOUNT_COLUMN = "tollsAmount";
    public static final String TOTAL_AMOUNT_COLUMN = "totalAmount";
}

package de.hfu.businessintelligence.service.support;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Optional;

import static de.hfu.businessintelligence.configuration.TableConfiguration.*;

@Slf4j
public class FilterService {

    private static final FilterService instance = new FilterService();

    private FilterService() {

    }

    public static FilterService getInstance(Dataset<Row> trips) {
        return instance;
    }

    public Dataset<Row> filterEverything(Dataset<Row> trips) {
        return trips
                .filter(this::isDropOffLongitudeNull)
                .filter(this::isDropOffLatitudeNull);
    }

    public Dataset<Row> filterNullValuesForDropOffLongitude(Dataset<Row> trips) {
        return trips.filter(this::isDropOffLongitudeNull);
    }

    public Dataset<Row> filterNullValuesForDropOffLatitude(Dataset<Row> trips) {
        return trips.filter(this::isDropOffLatitudeNull);
    }

    private boolean isDropOffLongitudeNull(Row row) {
        return Optional.ofNullable(row.getAs(DROP_OFF_LONGITUDE_COLUMN))
                .map(dropOffLongitude -> true)
                .orElseGet(() -> logNullValue(row.getAs(MEDALLION_COLUMN), DROP_OFF_LONGITUDE_COLUMN));
    }

    private boolean isDropOffLatitudeNull(Row row) {
        return Optional.ofNullable(row.getAs(DROP_OFF_LATITUDE_COLUMN))
                .map(dropOffLatitude -> true)
                .orElseGet(() -> logNullValue(row.getAs(MEDALLION_COLUMN), DROP_OFF_LATITUDE_COLUMN));
    }

    private boolean logNullValue(String id, String columnName) {
        log.info("Entry with ID "
                .concat(id)
                .concat("was removed as no value for column ")
                .concat(columnName).concat(" is null!")
        );
        return false;
    }
}

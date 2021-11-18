package de.hfu.businessintelligence.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.sql.Timestamp;

@Setter
@Getter
public class TripData implements Serializable {

    private String medallion;
    private String hackLicense;
    private String vendorId;
    private Integer rateCode;
    private String storeAndFwdFlag;
    private Timestamp pickupDateTime;
    private Timestamp dropOffDateTime;
    private Integer passengerCount;
    private Integer tripTimeInSeconds;
    private Double tripDistance;
    private Double pickupLongitude;
    private Double pickupLatitude;
    private Double dropOffLongitude;
    private Double dropOffLatitude;

}

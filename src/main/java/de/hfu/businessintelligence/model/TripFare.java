package de.hfu.businessintelligence.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.sql.Timestamp;

@Setter
@Getter
public class TripFare implements Serializable {

    private String medallion;
    private String hackLicense;
    private String vendorId;
    private Timestamp pickUpDatetime;
    private String paymentType;
    private Double fareAmount;
    private Double surchage;
    private Double mtaTax;
    private Double tipAmount;
    private Double tollsAmount;
    private Double totalAmount;

}

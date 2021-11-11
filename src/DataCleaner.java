import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

public class DataCleaner {

	final double SOUTHERNMOST_POINT = 40.496;
	final double NORTHERNMOST_POINT = 40.910;
	final double EASTERNMOST_POINT = -73.700;
	final double WESTERNMOST_POINT = -74.255;
	
	public DataCleaner() {
		
	}
	
	public Boolean isValidDrive(double tripTime, double tripDistance, double pickupLongitude, double pickupLatitude, double dropoffLongitude, double dropoffLatitude) {
		return 
				(
						tripTime > 0.0
						&& tripDistance > 0.0
						&& (pickupLongitude <= EASTERNMOST_POINT && pickupLongitude >= WESTERNMOST_POINT)
						&& pickupLatitude >= SOUTHERNMOST_POINT && pickupLatitude <= NORTHERNMOST_POINT 
						&& dropoffLongitude <= EASTERNMOST_POINT && dropoffLongitude >= WESTERNMOST_POINT
						&& dropoffLatitude >= SOUTHERNMOST_POINT && dropoffLatitude <= NORTHERNMOST_POINT
				);
	}
}

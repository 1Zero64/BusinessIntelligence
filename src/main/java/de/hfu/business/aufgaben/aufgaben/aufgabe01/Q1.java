package de.hfu.business.aufgaben.aufgaben.aufgabe01;

import de.hfu.business.aufgaben.utility.DataCleaner;
import de.hfu.business.aufgaben.utility.DoubleRounder;
import org.apache.spark.api.java.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.DoubleFunction;

public class Q1 {

	static DataCleaner dataCleaner = new DataCleaner();
		
	public static void main(String[] args) {

		String logFile = "/home/osboxes/data/NY_medium.csv";
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> nyDrivesRaw = sc.textFile(logFile).cache();
		
		
		System.out.println("Number of records before cleaning: " + nyDrivesRaw.count());
		System.out.println("Cleaning data...");
		
		JavaRDD<String> nyDrives = nyDrivesRaw.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				String[] attributes = s.split(",");
				Double tripTime = Double.parseDouble(attributes[8]);
				Double tripDistance = Double.parseDouble(attributes[9]);
				Double pickupLongitude = Double.parseDouble(attributes[10]);
				Double pickupLatitude = Double.parseDouble(attributes[11]);
				Double dropoffLongitude = Double.parseDouble(attributes[12]);
				Double dropoffLatitude = Double.parseDouble(attributes[13]);
				
				return dataCleaner.isValidDrive(tripTime, tripDistance, pickupLongitude, pickupLatitude, dropoffLongitude, dropoffLatitude);
			}
		});
		
		System.out.println("Date cleaned.");
		System.out.println(nyDrives.count() + " records remaining.");
		System.out.println("Records purged: " + (nyDrivesRaw.count() - nyDrives.count()));
		

		JavaRDD<String> drivesUnder30KM = nyDrives.filter(new Function<String, Boolean>() {
			public Boolean call(String arg0) throws Exception {
				String[] attributes = arg0.split(",");
				return Double.parseDouble(attributes[9]) < 30;
			}
		});

		JavaDoubleRDD drivesUnder30KMFareAmounts = drivesUnder30KM.mapToDouble(new DoubleFunction<String>() {
			public double call(String arg0) throws Exception {
				String[] attributes = arg0.split(",");
				return Double.parseDouble(attributes[attributes.length - 1]);
			}
		});

		JavaRDD<String> drivesUnder50KM = nyDrives.filter(new Function<String, Boolean>() {
			public Boolean call(String arg0) throws Exception {
				String[] attributes = arg0.split(",");
				return Double.parseDouble(attributes[9]) < 50 /** && Double.parseDouble(attributes[9]) >= 30 **/;
			}
		});

		JavaDoubleRDD drivesUnder50KMFareAmounts = drivesUnder50KM.mapToDouble(new DoubleFunction<String>() {
			public double call(String arg0) throws Exception {
				String[] attributes = arg0.split(",");
				return Double.parseDouble(attributes[attributes.length - 1]);
			}
		});
		
		JavaDoubleRDD allDrivesFareAmount = nyDrives.mapToDouble(new DoubleFunction<String>() {
			public double call(String arg0) throws Exception {
				String[] attributes = arg0.split(",");
				return Double.parseDouble(attributes[attributes.length - 1]);
			}
		});

		System.out.println("Average total amount for 30 km and under: " + DoubleRounder.round(drivesUnder30KMFareAmounts.mean(), 4));
		System.out.println("Average total amount for 50 km and under: " + DoubleRounder.round(drivesUnder50KMFareAmounts.mean(), 4));
		System.out.println("Average total amount for all drives: " + DoubleRounder.round(allDrivesFareAmount.mean(), 4) + "\n");

		sc.close();
	}
}
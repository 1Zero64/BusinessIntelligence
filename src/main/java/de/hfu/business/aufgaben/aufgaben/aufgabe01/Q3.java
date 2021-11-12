package de.hfu.business.aufgaben.aufgaben.aufgabe01;

import de.hfu.business.aufgaben.utility.DoubleRounder;
import org.apache.spark.api.java.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.DoubleFunction;

public class Q3 {

	public static void main(String[] args) {

		String logFile = "/home/osboxes/data/NY_medium.csv";
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> nyDrives = sc.textFile(logFile).cache();
		
		System.out.println(nyDrives.count());
		
		JavaDoubleRDD mphPerDrive = nyDrives.mapToDouble(new DoubleFunction<String>() {
			public double call(String arg0) throws Exception {
				String[] attributes = arg0.split(",");
					double seconds = Double.parseDouble(attributes[8]);
					double miles = Double.parseDouble(attributes[9]);
					double hours = seconds / 3600.0;
					if (hours == 0.0)
						return 0.0;
					return (double) (miles / hours);
			}
		});
		
		System.out.println("Average mph: " + DoubleRounder.round(mphPerDrive.mean(), 2));
		System.out.println(mphPerDrive.max() + "              " + mphPerDrive.min());
		
		sc.close();
	}
}
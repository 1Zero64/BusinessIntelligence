import org.apache.spark.api.java.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.codehaus.janino.Java;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;

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
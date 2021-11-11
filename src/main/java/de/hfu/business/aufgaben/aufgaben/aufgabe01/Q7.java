package de.hfu.business.aufgaben.aufgaben.aufgabe01;

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

public class Q7 {

	public static void main(String[] args) {

		String logFile = "/home/osboxes/data/NY-02_short.csv";
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> nyDrives = sc.textFile(logFile).cache();
		
		JavaDoubleRDD pickUpLatitudes = nyDrives.mapToDouble(new DoubleFunction<String>() {
			public double call(String arg0) throws Exception {
				String[] attributes = arg0.split(",");
				return Double.parseDouble(attributes[11]);
			}
		});
		
		JavaDoubleRDD southernLatitudes = pickUpLatitudes.filter(new Function<Double, Boolean>() {
			public Boolean call(Double arg0) throws Exception {
				return arg0 < 40.730610;
			}
		});
		
		System.out.println("In South (Latitude 40.730610 and less): " + southernLatitudes.count());
		System.out.println("In North (Latitude more than 40.730610): " + (pickUpLatitudes.count() - southernLatitudes.count()));
		System.out.println("Min: " + pickUpLatitudes.min());
		System.out.println("Max: " + pickUpLatitudes.max());
		
		sc.close();
	}
}
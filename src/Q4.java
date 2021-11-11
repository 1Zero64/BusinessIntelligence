import org.apache.spark.api.java.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.janino.Java;

import scala.Tuple2;

import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;

public class Q4 {

	public static void main(String[] args) {

		String logFile = "/home/osboxes/data/NY_medium.csv";
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> nyDrives = sc.textFile(logFile).cache();
		
		PairFunction<String, String, Double> keyData = new PairFunction<String, String, Double>() {
			public Tuple2<String, Double> call(String s) {
				String[] attributes = s.split(",");
				double seconds = Double.parseDouble(attributes[8]);
				double miles = Double.parseDouble(attributes[9]);
				double hours = seconds / 3600.0;
				if (hours == 0.0)
					return new Tuple2(s.split(",")[5].substring(0, 13) + ":00", 0.0);
				return new Tuple2(s.split(",")[5].substring(0, 13) + ":00", (miles / hours));
			}
		};

		JavaPairRDD<String, Double> pairs = nyDrives.mapToPair(keyData);
		
		JavaPairRDD<String, Tuple2<Double, Double>> valueCount = pairs.mapValues(value -> new Tuple2<Double, Double>(value,1.0));
		
	    JavaPairRDD<String, Tuple2<Double, Double>> reducedCount = valueCount.reduceByKey((tuple1,tuple2) ->  new Tuple2<Double, Double>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)); 
	    
	    PairFunction<Tuple2<String, Tuple2<Double, Double>>,String,Double> getAverageByKey = (tuple) -> {
	        Tuple2<Double, Double> val = tuple._2;
	        double total = val._1;
	        double count = val._2;	
	        Tuple2<String, Double> averagePair = new Tuple2<String, Double>(tuple._1, total / count);
	        return averagePair;
	     };
	    JavaPairRDD<String, Double> averagePair = reducedCount.mapToPair(getAverageByKey);
	    
	    PairFunction<Tuple2<String, Double>, String, Double> hourData = new PairFunction<Tuple2<String, Double>, String, Double>() {
			public Tuple2<String, Double> call(Tuple2<String, Double> t) {
				return new Tuple2(t._1().substring(11, 16), t._2());
			}
		};
		
		JavaPairRDD<String, Double> dayCount = averagePair.mapToPair(hourData);
		
		JavaPairRDD<String, Tuple2<Double, Double>> daysCounted = dayCount.mapValues(value -> new Tuple2<Double, Double>(value,1.0));
		
		JavaPairRDD<String, Tuple2<Double, Double>> reducedDays= daysCounted.reduceByKey((tuple1,tuple2) ->  new Tuple2<Double, Double>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));

		JavaPairRDD<String, Double> averagePerDayAndTime = reducedDays.mapToPair(getAverageByKey);
		
		JavaPairRDD<String, Double> sortedAveragePerDayAndTime = averagePerDayAndTime.sortByKey(true);
		
		sortedAveragePerDayAndTime.foreach(data -> {
			System.out.println("Hour: " + data._1() + "\tAverage mph: " + DoubleRounder.round(data._2(), 2) + " mph");
		});
		
		
		sc.close();
	}
}
package de.hfu.businessintelligence;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Q5 {

	public static void main(String[] args) {

		String logFile = "/home/osboxes/data/NY_medium.csv";
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> nyDrives = sc.textFile(logFile).cache();
		
		PairFunction<String, String, Integer> keyData = new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2(s.split(",")[5].substring(0, 13) + ":00", 1);
			}
		};

		JavaPairRDD<String, Integer> allDrives = nyDrives.mapToPair(keyData);
		
	    JavaPairRDD<String, Integer> drivesPerDayAndHours = allDrives.reduceByKey((d1, d2) ->  d1 + d2);

	    PairFunction<Tuple2<String, Integer>, String, Tuple2<Integer, Integer>> keyHours = new PairFunction<Tuple2<String,Integer>, String, Tuple2<Integer,Integer>>() {
	    	public Tuple2<String, Tuple2<Integer, Integer>> call(Tuple2<String, Integer> t) {
				return new Tuple2(t._1.substring(11, 16), new Tuple2(t._2, 1));
			}
	    };
	    
	    JavaPairRDD<String, Tuple2<Integer, Integer>> countedHoursAndDays = drivesPerDayAndHours.mapToPair(keyHours);
	    
	    JavaPairRDD<String, Tuple2<Integer, Integer>> reducedHoursAndDays = countedHoursAndDays.reduceByKey((tuple1,tuple2) ->  new Tuple2<Integer, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
	    
	    PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Double> getAverageByKey = (value)-> {
	    	Tuple2<Integer, Integer> val = value._2;
	    	int drives = val._1;
	    	int count = val._2;
	    	return new Tuple2<String, Double>(value._1, (double) drives/count);
	    };
	    
	    JavaPairRDD<String, Double> drivesAveragePerHour = countedHoursAndDays.mapToPair(getAverageByKey);
	    
	    JavaPairRDD<String, Double> sortedDrivesAveragePerHour = drivesAveragePerHour.sortByKey(true);
	    
	    sortedDrivesAveragePerHour.foreach(data -> {
	        System.out.println("Hour: " + data._1() + "\tNumber of drives average per hour average per day: " + data._2());
	    });
		
		sc.close();
	}
}
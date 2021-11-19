package de.hfu.businessintelligence;

import de.hfu.businessintelligence.configuration.SparkConfiguration;
import de.hfu.businessintelligence.service.mapper.TripMapperService;
import de.hfu.businessintelligence.service.task.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class Application {

    public static void main(String[] args) {
        SparkSession spark = SparkConfiguration.getInstance().sparkSession();
        Dataset<Row> trips = TripMapperService.getInstance(spark).getAllTrips();
        trips.printSchema();

        List<TaskService> tasks = List.of(
                FirstTaskService.getInstance(spark),
                SecondTaskService.getInstance(spark),
                ThirdTaskService.getInstance(spark),
                FourthTaskService.getInstance(spark),
                FifthTaskService.getInstance(spark),
                SixthTaskService.getInstance(spark),
                SeventhTaskService.getInstance(spark),
                EighthTaskService.getInstance(spark),
                NinthTaskService.getInstance(spark),
                TenthTaskService.getInstance(spark),
                EleventhTaskService.getInstance(spark)
        );
        tasks.forEach(TaskService::executeTask);
    }
}
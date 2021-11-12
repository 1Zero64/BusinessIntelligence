package de.hfu.businessintelligence;

import de.hfu.businessintelligence.configuration.SparkConfiguration;
import de.hfu.businessintelligence.service.mapper.TripMapperService;
import de.hfu.businessintelligence.service.task.FirstTaskService;
import de.hfu.businessintelligence.service.task.TaskService;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class Application {

    public static void main(String[] args) {
        SparkSession spark = SparkConfiguration.getInstance().sparkSession();
        TripMapperService.getInstance(spark).getAllTrips();
        List<TaskService> tasks = List.of(
                FirstTaskService.getInstance(spark)
        );
        tasks.forEach(TaskService::executeTask);
    }
}
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

@Slf4j
public class App {

    private static final String CLUSTER_CONNECTION = "local";
    private static final String CLUSTER_ID = "bi";

    public static void main(String[] args) {
        log.info("Trying to start spark server...");
        SparkConf conf = new SparkConf().setMaster(CLUSTER_CONNECTION).setAppName(CLUSTER_ID);
        JavaSparkContext sc = new JavaSparkContext(conf);
        log.info("Spark Server was successfully started.");
        log.info("Trying to shutdown spark server...");
        sc.close();
        log.info("Spark Server was shutdown.");
    }
}

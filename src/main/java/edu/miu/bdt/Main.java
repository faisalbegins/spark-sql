package edu.miu.bdt;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.set("hive.metastore.uris", "thrift://localhost:9083");
        sparkConf.set("hive.exec.scratchdir", "/tmp/myhive");

        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkSQL-Hive")
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();

        // switch to twitter database
        sparkSession.sqlContext().sql("USE twitter");

        // TOP 100 HASHTAG
        Dataset<Row> top100HashTag = sparkSession.sql(Queries.TOP_100_HASHTAG);
        top100HashTag.write().csv(args[0] + "/Top100HashTag");

        // Find all the distinct users
        Dataset<Row> all = sparkSession.sql(Queries.ALL);
        all.select(col("username")).distinct().write().csv(args[0] + "/DistinctUsers");

        // Create table and put user count directly into it
        sparkSession.sql(Queries.CREATE_TABLE);
        sparkSession.sql(Queries.TOTAL_USERS);
    }
}

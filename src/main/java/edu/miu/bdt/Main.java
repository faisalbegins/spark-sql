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

        // execute query
        Dataset<Row> result = sparkSession.sql(Sql.QUERY);

        result.write().csv(args[0] + "/result1");

        // process on top of query result
        result.select(col("hashtag")).distinct().write().csv(args[0] + "/result2");
    }
}

package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IDEA
 * User: vector
 * Data: 2018/4/20 0020
 * Time: 9:58
 * Description: spark-submit --class com.spark.SparkPI --master local /ssd/spark/code/spark-pi/spark-pi-1.0-SNAPSHOT.jar 10
 */
public class SparkPI {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Spark PI");
//        SparkConf conf = new SparkConf().setAppName("Spark PI");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;

        int n = 100000 * slices;

        List<Integer> integers = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            integers.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(integers);
        Integer count = dataSet.map((Function<Integer, Integer>) integer -> {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            return (x * x + y * y < 1) ? 1 : 0;
        }).reduce((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);

        System.out.println("Pi is roughly " + 4.0 * count / n);

        jsc.stop();


    }
}

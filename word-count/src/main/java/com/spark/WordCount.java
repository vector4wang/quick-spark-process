package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @Author: wangxc
 * @GitHub: https://github.com/vector4wang
 * @CSDN: http://blog.csdn.net/qqhjqs?viewmode=contents
 * @BLOG: http://vector4wang.tk
 * @wxid: BMHJQS
 */
public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCount");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> javaRDD = context.textFile("D:\\develop\\gitspace\\quick-spark-process\\word-count\\src\\main\\resources\\blsmy.txt");
        JavaRDD<String> strings = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairRDD<String, Integer> pairs = strings.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> reduceByKey = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<Integer, String> integerStringJavaPairRDD = reduceByKey.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });


        List<Tuple2<String, Integer>> top = integerStringJavaPairRDD.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                return new Tuple2<>(tuple._2, tuple._1);
            }
        }).top(10, new Comparator<Tuple2<String, Integer>>() {
            @Override
            public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                return o1._2 - o2._2;
            }
        });

        for (Tuple2<String, Integer> tuple : top) {
            System.out.println(tuple._1 + ": " + tuple._2);
        }

    }
}

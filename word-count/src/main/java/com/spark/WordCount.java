package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: wangxc
 * @GitHub: https://github.com/vector4wang
 * @CSDN: http://blog.csdn.net/qqhjqs?viewmode=contents
 * @BLOG: http://vector4wang.tk
 * @wxid: BMHJQS
 * <p>
 * 《巴黎圣母院》英文版的统计 用于本机学习与测试
 */
public class WordCount {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("WordCount")
                .set("spark.cores.max", "1")
                .set("spark.eventLog.enabled", "true");
        Tuple2<String, String>[] all = conf.getAll();
        for (Tuple2<String, String> stringStringTuple2 : all) {
            System.out.println(stringStringTuple2._1 + ": " + stringStringTuple2._2);
        }
        JavaSparkContext context = new JavaSparkContext(conf);
        // 用于idea测试
		String classFilePath = WordCount.class.getResource("/blsmy.txt").getPath();

		JavaRDD<String> javaRDD = context.textFile(classFilePath);
//        JavaRDD<String> javaRDD = context.textFile("file:///mnt/data/blsmy.txt"); -- 用于集群运行(前提，运行的各节点都需要有此文件)
//        JavaRDD<String> javaRDD = context.textFile("hdfs://spark-master:9000/wordcount/blsmy.txt");

//
		JavaRDD<String> words = javaRDD.flatMap((FlatMapFunction<String, String>) s -> {
			String[] split = s.split(" ");
			List<String> strings = Arrays.asList(split);
			return strings.iterator();
		});

        JavaPairRDD<String, Integer> pairs = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> reduceByKey = pairs.reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);

        JavaPairRDD<Integer, String> integerStringJavaPairRDD = reduceByKey.mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) stringIntegerTuple2 -> new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1));


        JavaPairRDD<String, Integer> mapToPair = integerStringJavaPairRDD.sortByKey(false).mapToPair((PairFunction<Tuple2<Integer, String>, String, Integer>) tuple -> new Tuple2<>(tuple._2, tuple._1));

        mapToPair.foreach((VoidFunction<Tuple2<String, Integer>>) tuple -> System.out.println(tuple._1 + ": " + tuple._2));
    }
}

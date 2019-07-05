package com.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 *
 * @author vector
 * @date: 2019/7/4 0004 17:05
 */
public class SparkStreamingDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("NetWorkWordCount");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 在目标机器人 执行 `nc -lp 9999` 打开9999端口，然后可以输入一些字符串
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("192.168.1.33", 9999);
		JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairDStream = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> wordCounts = pairDStream.reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();
        jsc.start();
        jsc.awaitTermination();

    }
}

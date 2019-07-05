package com.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Created with IDEA
 * User: vector
 * Data: 2018/4/20 0020
 * Time: 10:41
 * Description:
 */
public class JavaSparkSql {
    public static void main(String[] args) {
        String classFilePath = JavaSparkSql.class.getResource("/people.json").getPath();


        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset<Row> df = spark.read().json(classFilePath);

        /**
         * 显示表的内容 (前20条)
         */
        df.show();

        /**
         * 打印节点 (tree 结构)
         */
        df.printSchema();

        /**
         *  选择属性显示 并对属性做简单操作
         */
        df.select(df.col("name"), df.col("age").plus(1)).show();

        /**
         * 简单的过滤
         */
        df.filter(df.col("age").gt(21)).show();

        /**
         * 分组统计
         */
        df.groupBy("age").count().show();


        df.createOrReplaceTempView("peopleTmp");


        // SQL can be run over RDDs that have been registered as tables.
        Dataset<Row> teenagers = spark.sql("select name,age from peopleTmp where age > 13 and age <=19");
        teenagers.toJavaRDD().map(row -> "Name: " + row.getString(0)).collect().forEach(System.out::println);

        /**
         * parquet file
         */
        teenagers.write().mode(SaveMode.Overwrite).parquet("people.parquet");

        /**
         * 对parquet文件做些简单的操作
         *
         */
        System.out.println("=== Data source: Parquet File ===");


        Dataset<Row> parquet = spark.read().parquet("people.parquet");

        parquet.show();

        parquet.createOrReplaceTempView("parquetPeople");

        Dataset<Row> teenagers2 = spark.sql("select name from parquetPeople where age > 13 and age <= 19");

        teenagers2.show();
    }
}

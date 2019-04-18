# quick-spark-process
学习spark的相关示例

[![LICENSE](https://img.shields.io/badge/license-Anti%20996-blue.svg)](https://github.com/996icu/996.ICU/blob/master/LICENSE)


### word-count
最简单也是最经典的例子
后面搭了spark集群 并使用了hdfs来存储文件，有几点需要注意
#### 文件的调用方式
```java
context.textFile("D:\\data\\spark\\blsmy.txt");  -- 用于idea测试
context.textFile("file:///mnt/data/blsmy.txt"); -- 用于集群运行(前提，运行的各节点都需要有此文件)
context.textFile("hdfs://spark-master:9000/wordcount/blsmy.txt"); -- 使用hdfs调用文件
```
#### 日志输出的位置
在页面中，输出的日志有sterr和stdout两种，在stdout可以查看程序中输出的内容。如果你在程序中使用了println(....)输出语句，这些信息会在stdout文件里面显示；其余的Spark运行日志会在stderr文件里面显示。
也可以直接进行日志文件进行查看，如：
```bash
/spark/software/spark/work/app-20180428142302-0003/0/stdout
/spark/software/spark/work/app-20180428142302-0003/0/stderr
```
#### 启动的方式
```bash
bin/spark-submit \ 
    --master spark://spark-master:7077 \
    --driver-memory 1g \
    --executor-cores 1 \
    --class com.spark.WordCount \
    simple/word-count-1.0-SNAPSHOT.jar
```




### spark-pi
也是一个比较经典的栗子

### spark-sql
使用sparksql做的简单操作

package com.alahr.spark.example.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        String fileDir;
        if (null != args && args.length >= 2) {
            conf.setAppName(args[0]);
            fileDir = args[1];
            System.out.println(args[0] + " : " + args[1]);
        } else {
            conf.setAppName("word-count").setMaster("local[2]");
            fileDir = "english.txt";
        }
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(fileDir);
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordCounts = ones.reduceByKey((a, b) -> a + b);
        wordCounts.sortByKey().collect().forEach(t -> System.out.println(t._1() + ": " + t._2()));

        sc.close();
    }
}

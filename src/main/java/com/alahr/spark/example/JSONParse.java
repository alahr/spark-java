package com.alahr.spark.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class JSONParse {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf().setAppName("json-parse").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sc);

        JavaRDD<String> lines = sparkContext.textFile("json.txt");

        JavaPairRDD<String, Integer> ones = lines.mapToPair(line -> {
            JSONObject obj = JSONObject.parseObject(line);
            String date = obj.getString("date").substring(0, 16);
            return new Tuple2<String, Integer>(date, 1);
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((a, b) -> a + b);

        //true: asc; false: desc
        counts.sortByKey(true).foreach(t -> System.out.println(t._1 + " : " + t._2));

        sparkContext.close();
    }
}

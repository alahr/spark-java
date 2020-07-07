package com.alahr.spark.example.rdd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class GroupTest {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf().setAppName("group-test").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sc);

        JavaRDD<String> rdd = sparkContext.textFile("group.txt");

        JavaRDD<JSONObject> records = rdd
                .flatMap(line -> Arrays.asList(line.split(";")).iterator())
                .map(r -> JSON.parseObject(r));

        records.groupBy(new Function<JSONObject, Object>() {
            @Override
            public Object call(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("id");
            }
        });


        sparkContext.close();
    }
}

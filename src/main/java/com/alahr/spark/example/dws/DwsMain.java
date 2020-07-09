package com.alahr.spark.example.dws;

import com.alahr.spark.example.dws.dto.Person;
import com.alibaba.fastjson.JSON;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class DwsMain {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("DwsMain")
                .getOrCreate();

        Person p1 = new Person("shanghai", "Tom", "spark", 58.0D);
        Person p2 = new Person("shanghai", "Tom", "mongo", 52.0D);
        Person p3 = new Person("shanghai", "Tom", "hive", 53.0D);
        Person p4 = new Person("beijing", "Lucy", "hadoop", 40.0D);
        Person p5 = new Person("beijing", "Lucy", "mysql", 41.0D);
        Person p6 = new Person("chongqing", "Dav", "oracle", 30.0D);
        Person p7 = new Person("tianjin", "Jet", "redis", 58.0D);
        Person p8 = new Person("tianjin", "Jet", "elasticsearch", 58.0D);

        List<Person> personList = Arrays.asList(p1, p2, p3, p4, p5, p6, p7, p8);

        Dataset<Person> dataset = sparkSession.createDataset(personList, Encoders.bean(Person.class));

        dataset.javaRDD()
                .groupBy( person -> person.getCity()+"_"+person.getName())
                .foreach(t -> {
                    String key = t._1;
                    Iterator<Person> iterator = t._2.iterator();
                    List<Person> pList = new ArrayList<>();
                    while (iterator.hasNext()){
                        Person p = iterator.next();
                        pList.add(p);
                    }
                    System.out.println(key+": "+ JSON.toJSONString(pList));
                });
        sparkSession.close();
    }
}

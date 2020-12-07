package com.alahr.spark.example.merge;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.URI;

public class SmallFilesSpark {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("SmallFilesSpark")
                .getOrCreate();

        String path1 = "hdfs://path/hive_db/alahr.db/person/day=20201207";
        String path2 = "hdfs://path/hive_db/alahr.db/person/tmp/day=20201207";

        Integer numPartitions1 = getNumPartitions(sparkSession, path1);
        Dataset<Row> originDS = sparkSession.read().parquet(path1);
        originDS.repartition(numPartitions1).write().parquet(path2);

        Integer numPartitions2 = getNumPartitions(sparkSession, path2);
        Dataset<Row> mergeDS = sparkSession.read().parquet(path2);
        mergeDS.repartition(numPartitions2).write().parquet(path1);
        /*
        SET hive.merge.size.per.task=256000000;
        SET hive.merge.smallfiles.avgsize=128000000;
        SET hive.merge.mapfiles=true;
        SET hive.merge.mapredfiles=true;

        Insert overwrite table alahr.person partition(day='20201207')
        select
        id,name,age,address
        from alahr.person
        where day='20201207'
         */
        sparkSession.close();
    }

    private static Integer getNumPartitions(SparkSession sparkSession, String partitionPath) {
        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfc://path"), sparkSession.sparkContext().hadoopConfiguration());
            FileStatus[] fileStatuses = fileSystem.globStatus(new Path(partitionPath + "/*"));

            Long totalBytes = 0L;
            for (FileStatus fileStatus : fileStatuses) {
                totalBytes += fileStatus.getLen();
            }
            Long numPartitions = totalBytes / (100 * 1024 * 1024);
            return numPartitions.intValue() + 1;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

}

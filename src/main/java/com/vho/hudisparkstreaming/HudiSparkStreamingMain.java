package com.vho.hudisparkstreaming;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.*;
import org.junit.Test;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.spark.sql.functions.expr;

public class HudiSparkStreamingMain {

  private static final String tableType = COPY_ON_WRITE.name();
  private static final String tableName = "streaming";
  private static final String tablePath = "hdfs://localhost:8020/user/hive/warehouse/hoodie/" + tableName;

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder()
      .appName("hoodie spark streaming")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate();


    try {
      readAndWriteStream(spark);
      readFromTableAndTransformStream(spark);
    } catch (StreamingQueryException e) {
      e.printStackTrace();
    }
  }

  private static void readFromTableAndTransformStream(SparkSession spark) {

  }

  private static void readAndWriteStream(SparkSession sparkSession) throws StreamingQueryException {
    DataStreamWriter<Row> writer = sparkSession.readStream().format("rate").option("rowsPerSecond", 10).load()
      .selectExpr("if (cast(rand(10) as int) % 2 == 0, 'Open', 'Close') as action")
      .withColumn("date", expr("cast(concat('2019-04-', cast(rand(5) * 30 as int) + 1) as date)"))
      .withColumn("device_id", expr("cast(rand(5) * 500 as int)"))
      .writeStream()
      .format("org.apache.hudi")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), tableType)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "device_id")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "date")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "date")
      .option(HoodieWriteConfig.TABLE_NAME, tableName)
      .option("checkpointLocation", tablePath + "/.checkpoint")
      .outputMode(OutputMode.Append())
      .partitionBy("date")
      .trigger(Trigger.ProcessingTime("60 seconds"));

    writer.start(tablePath).awaitTermination();
  }
}

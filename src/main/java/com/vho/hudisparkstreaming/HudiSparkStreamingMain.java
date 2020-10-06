package com.vho.hudisparkstreaming;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.*;

import static org.apache.hudi.common.model.HoodieTableType.*;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;

public class HudiSparkStreamingMain {

  private static final String tableType = COPY_ON_WRITE.name();
  private static final String basepath = "hdfs://localhost:8020/user/hive/warehouse/hoodie";
  private static final String cowTableName = "hudi_streaming_cow";
  private static final String morTableName = "hudi_streaming_mor";
  private static final String cowTablePath = basepath + "/streaming_cow";
  private static final String morTablePath = basepath + "/streaming_mor";
//  private static final String cowCheckptPath = basepath + "/.checkpoint/streaming_cow";
//  private static final String morCheckptPath = basepath + "/.checkpoint/streaming_mor";
  private static final String cowTablePathLocal = "/tmp/hoodie/hoodie_streaming_cow";

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder()
      .appName("hoodie spark streaming")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate();

    try {
//      writeStreamCOW(sourceStream(spark), cowTablePath, cowCheckptPath);
//      writeStreamCOW(sourceStream(spark), cowTablePath);
//      writeStreamMOR(sourceStream(spark));
      testReadFromHudiTable(spark, cowTablePathLocal);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void testReadFromHudiTable(SparkSession spark, String tablePath) {
    Dataset<Row> df = spark.read().format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
      .load(tablePath);

    df.printSchema();
    df.createOrReplaceTempView("cow");
    final Dataset<Row> query = spark.sql("select id, dt from cow where year = 2020");
    query.explain(true);
    query.show();
  }

  private static Dataset<Row> sourceStream(SparkSession spark) {
    return spark.readStream().format("rate").option("rowsPerSecond", 10).load()
      .selectExpr("if (cast(rand(10) * 2 as int) % 2 == 0, 'Open', 'Close') as action")
      .withColumn("year", lit(2020))
      .withColumn("month", lit(9))
      .withColumn("day", expr("cast(rand(5)*5 as int) + 1"))
      .withColumn("ts", expr("current_timestamp()"))
      .withColumn("dt", expr("cast(concat(year, '-', month, '-', day) as date)"))
      .withColumn("device_id", expr("cast(rand(5) * 5 as int) + 1"));
  }

  private static void writeStreamMOR(Dataset<Row> inputStream) throws StreamingQueryException {
    DataStreamWriter<Row> writer = inputStream.writeStream()
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), MERGE_ON_READ.name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "device_id")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "date")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "date")
      .option(HoodieWriteConfig.TABLE_NAME, morTableName)
      .option("checkpointLocation", morTablePath + "/.checkpoint")
      .outputMode(OutputMode.Append())
      .partitionBy("date")
      .trigger(Trigger.ProcessingTime("15 seconds"));

    writer.start(morTablePath).awaitTermination();
  }

  private static void writeStreamCOW(Dataset<Row> inputStream, String tablePath) throws StreamingQueryException {
    final String partitionPath = "year:SIMPLE,month:SIMPLE,day:SIMPLE";
    DataStreamWriter<Row> writer = inputStream.writeStream()
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), COPY_ON_WRITE.name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "device_id")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), partitionPath)
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(), CustomKeyGenerator.class.getCanonicalName())
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "ts")
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY(), "true")
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), cowTableName)
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), partitionPath)
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(), MultiPartKeysValueExtractor.class.getCanonicalName())
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(), "true")
      .option(HoodieWriteConfig.TABLE_NAME, cowTableName)
      .option("checkpointLocation", tablePath + "/.checkpoint")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("60 seconds"));

    writer.start(tablePath).awaitTermination();
  }
}

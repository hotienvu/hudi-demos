package com.vho.hudisparkstreaming;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.spark.sql.functions.*;


public class StructuredStreamingTest {

  @Test
  public void test() throws StreamingQueryException, ExecutionException, InterruptedException {
    SparkSession spark = SparkSession.builder()
      .appName("structured streaming test")
      .master("local[*]")
      .config("hive.metastore.uris", "thrift://hivemetastore:9083")
      .config("dfs.client.use.datanode.hostname", "true")
      .config("dfs.replication", "1")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate();

    Configuration conf = new Configuration();

    String schema = "entityId String, scanId String";
    Dataset<Row> input = spark.readStream()
      .schema(StructType.fromDDL(schema))
      .json("file:///tmp/blackduck/raw/*.json")
      .groupBy("entityId").count();

    Dataset<Row> input2 = spark.readStream()
      .format("rate")
      .option("rowsPerSecond", "1")
      .option("numPartitions", "1")
      .load()
      .selectExpr("value", "if(value %2 == 0, 'open', 'close') as action")
      .withColumn("dt", expr("current_date()"))
      .withColumn("ts", expr("current_timestamp()"));

    Dataset<Row> kafkaInput = spark.readStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
      .withColumn("value", expr("cast(value as STRING)"))
      .withColumn("dt", expr("current_date()"));


    StreamingQuery saveRaw = kafkaInput.writeStream()
      .format("json")
      .partitionBy("dt")
      .option("checkpointLocation", "file:///tmp/blackduck/raw/_checkpoint")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start("file:///tmp/blackduck/raw");

    Dataset<Row> fileInput = spark.readStream()
      .schema(StructType.fromDDL("value String, topic String, timestamp Timestamp"))
      .format("json")
      .load("file:///tmp/blackduck/downloaded/**/*.json")
      .withColumn("dt", expr("date_format(current_date(), 'yyyy/MM/dd')"))
      .withColumn("_value", split(col("value"), ","))
      .withColumn("id", when(col("_value").getItem(0).equalTo(""), "undefined")
                                  .otherwise(col("_value").getItem(0)))
      .withColumn("value", col("_value").getItem(1))
      .drop("_value")
      .coalesce(1);

    StreamingQuery etl = fileInput.writeStream()
      .format("json")
      .partitionBy("dt")
      .option("checkpointLocation", "file:///tmp/blackduck/imports/_checkpoint")
      .start("file:///tmp/blackduck/imports");

    StreamingQuery hoodieWriter = fileInput.writeStream()
      .format("org.apache.hudi")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY(), HoodieTableType.COPY_ON_WRITE.name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "id")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "dt")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
      .option(DataSourceWriteOptions.STREAMING_IGNORE_FAILED_BATCH_OPT_KEY(), "false")
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY(), "true")
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY(), "true")
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), "hoodie")
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), "dt")
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY(), "jdbc:hive2://hiveserver:10000")
      .option(HoodieWriteConfig.TABLE_NAME, "hoodie")
      .option("checkpointLocation", "hdfs://namenode:8020/tmp/blackduck/hoodie/_checkpoint")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start("hdfs://namenode:8020/tmp/blackduck/hoodie/");

    StreamingQuery downloadRaw = kafkaInput
      .writeStream()
      .outputMode(OutputMode.Append())
      .partitionBy("dt")
      .format("json")
      .option("path", "file:///tmp/blackduck/downloaded/")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", "file:///tmp/blackduck/downloaded/_kafkacheckpoint")
      .start();

    ExecutorService es = Executors.newFixedThreadPool(4);
    Future<Void> f1 = es.submit(() -> {
      downloadRaw.awaitTermination();
      return null;
    });

    Future<Void> f2 = es.submit(() -> {
      etl.awaitTermination();
      return null;
    });

    Future<Void> f3 = es.submit(() -> {
      saveRaw.awaitTermination();
      return null;
    });

    Future<Void> f4 = es.submit(() -> {
      hoodieWriter.awaitTermination();
      return null;
    });

    fileInput.writeStream()
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "file:///tmp/blackduck/imports/_consolecheckpoint")
      .start()
      .awaitTermination();

    f1.get();
    f2.get();
    f3.get();
    f4.get();
  }
}

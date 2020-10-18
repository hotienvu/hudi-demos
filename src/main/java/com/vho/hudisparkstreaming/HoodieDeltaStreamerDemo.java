package com.vho.hudisparkstreaming;

import com.beust.jcommander.JCommander;
import org.apache.commons.io.FileUtils;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;

public class HoodieDeltaStreamerDemo {
  private static Logger LOG = LoggerFactory.getLogger(HoodieDeltaStreamerDemo.class);
  private static final String inputPath = "/tmp/hoodie/timeline_test/input";

  /**
   * --target-base-path file:///tmp/hoodie/timeline_test/cow
   * --target-table hoodie_cow
   * --table-type COPY_ON_WRITE
   * --props file:///.../dfs-source.properties
   * --source-class org.apache.hudi.utilities.sources.ParquetDFSSource
   * --source-ordering-field ts
   * --op UPSERT
   * --continuous
   * --min-sync-interval-seconds 30
   */
  public static void main(String[] args) throws IOException {
    HoodieDeltaStreamer.Config cfg = getConfig(args);
//    FileUtils.deleteDirectory(new File(inputPath));
//    FileUtils.deleteDirectory(new File(cfg.targetBasePath));
    Files.createDirectories(Paths.get(inputPath));
    Files.createDirectories(Paths.get(cfg.targetBasePath));

    SparkSession spark = SparkSession.builder()
      .appName("hoodie timeline demo")
      .master("local[2]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate();

    ExecutorService es = Executors.newFixedThreadPool(2);
    Future<?> read = es.submit(() -> generateInput(spark));
    Future<?> write = es.submit(() -> streamToHoodieTable(cfg, spark));
    try {
      read.get();
      write.get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    } finally {
      es.shutdown();
    }
  }

  private static void streamToHoodieTable(HoodieDeltaStreamer.Config cfg, SparkSession spark) {
    try {
      HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(cfg,
        JavaSparkContext.fromSparkContext(spark.sparkContext()));
      HoodieDeltaStreamer.DeltaSyncService deltaSyncService = deltaStreamer.getDeltaSyncService();
      deltaSyncService.start((error) -> {
        LOG.error("DeltaSync shutdown. Closing write client. Error: {}", error);
        deltaSyncService.close();
        return true;
      });
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static HoodieDeltaStreamer.Config getConfig(String[] args) {
    HoodieDeltaStreamer.Config cfg = new HoodieDeltaStreamer.Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    return cfg;
  }

  private static void generateInput(SparkSession spark) {
    Dataset<Row> inputStream =  spark.readStream().format("rate").option("rowsPerSecond", 10000).load()
      .withColumn("year", lit(2020))
      .withColumn("month", expr("cast(rand(0)*2 as int) + 1"))
      .withColumn("day", expr("cast(rand(0)*3 as int) + 1"))
      .withColumn("ts", expr("current_timestamp()"))
      .withColumn("dt", expr("concat(year, '-', month, '-', day)"))
      .withColumn("id", expr("cast(rand(0) * 10000 as int) + 1"))
      .withColumn("state", expr("if(cast(rand(0)*2 as int)  ==  0, 'Open', 'Closed') as state"));


    StreamingQuery stream = inputStream.writeStream()
      .format("parquet")
      .option("checkpointLocation", inputPath + "/.checkpoint")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("60 seconds"))
      // DeltaStreamer currently don't support directory partitioning
       .partitionBy("dt")
      .start(inputPath);

    try {
      stream.awaitTermination();
    } catch (StreamingQueryException e) {
      e.printStackTrace();
      stream.stop();
    }
  }
}

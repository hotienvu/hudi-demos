package com.vho.hudisparkstreaming;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.*;

class HoodieSparkDemo {
  public HoodieConfig config;

  public HoodieSparkDemo(HoodieConfig config) {
    this.config = config;
  }

  private static class SparkSessionHolder {
    private static final SparkSession INSTANCE = SparkSession.builder()
      .appName("hoodie multi-partition demo")
      .master("local[2]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate();
  }

  SparkSession spark() {
    return SparkSessionHolder.INSTANCE;
  }

  public void writeStream(Dataset<Row> input) {
    DataStreamWriter<Row> writer = input.writeStream()
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), config.getTableType().name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), config.getRecordKeyField())
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), config.getPartitionFields())
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(), config.getKeyGenerator())
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), config.getPreCombineFields())
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY(), config.isEnableHiveSync())
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), config.getTableName())
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), "year,month,day")
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(), MultiPartKeysValueExtractor.class.getCanonicalName())
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(), "true")
      .option(HoodieWriteConfig.TABLE_NAME, config.getTableName())
      .option("path", config.getBasePath())
      .option("checkpointLocation", config.getBasePath() + "/.checkpoint")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("60 seconds"));

    StreamingQuery stream = writer.start();
    try {
      stream.awaitTermination();
    } catch (StreamingQueryException e) {
      e.printStackTrace();
      stream.stop();
    }
  }
}

package com.vho.hudisparkstreaming;

import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;

public class HoodieMultiPartitionDemo extends HoodieSparkDemo {
  public HoodieMultiPartitionDemo(HoodieConfig config) {
    super(config);
  }

  @Override
  SparkSession sparkSession() {
    return SparkSession.builder()
      .appName("hoodie multi-partition demo")
      .master("local[2]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate();
  }

  @Override
  Dataset<Row> inputStream() {
    return sparkSession().readStream().format("rate").option("rowsPerSecond", 10).load()
      .withColumn("year", lit(2020))
      .withColumn("month", expr("cast(rand(5)*2 as int) + 1"))
      .withColumn("day", expr("cast(rand(5)*3 as int) + 1"))
      .withColumn("ts", expr("current_timestamp()"))
      .withColumn("dt", expr("cast(concat(year, '-', month, '-', day) as date)"))
      .withColumn("id", expr("cast(rand(5) * 5 as int) + 1"));
  }

  public static void main(String[] args) {
    HoodieConfig config = HoodieConfig.builder()
      .tableName("hoodie_streaming_cow")
      .tableType(COPY_ON_WRITE)
      .basePath("/tmp/hoodie/cow")
      .recordKeyField("id")
      .keyGenerator(CustomKeyGenerator.class.getCanonicalName())
      .enableHiveSync(true)
      .hiveStylePartitioning(true)
      .partitionFields("year:SIMPLE,month:SIMPLE,day:SIMPLE")
      .hivePartitionFields("year,month,day")
      .hivePartitionExtractorClass(MultiPartKeysValueExtractor.class.getCanonicalName())
      .build();

    HoodieMultiPartitionDemo demo = new HoodieMultiPartitionDemo(config);
    demo.writeStream();
  }
}

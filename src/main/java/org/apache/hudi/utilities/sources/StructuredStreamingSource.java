package org.apache.hudi.utilities.sources;


import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.RowSource;
import org.apache.hudi.utilities.sources.helpers.DFSPathSelector;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class StructuredStreamingSource extends RowSource {
  public StructuredStreamingSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession, SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    DataSourceUtils.checkRequiredProperties(props, Arrays.asList(DFSPathSelector.Config.ROOT_INPUT_PATH_PROP));


  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    String format = props.getString("hoodie.deltastreamer.source.structured-streaming.format");

    final String STRUCTURED_STREAMING_OPTION_PREFIX = "hoodie.deltastreamer.source.structured-streaming.options.";
    Map<String, String> options = new HashMap<>();
    for (String key: props.stringPropertyNames()) {
      if (key.startsWith(STRUCTURED_STREAMING_OPTION_PREFIX)) {
        options.put(key.substring(STRUCTURED_STREAMING_OPTION_PREFIX.length()), props.getProperty(key));
      }
    }

    StructType schema = sparkSession.read().format(format).load(options.get("path")).schema();
    final Dataset<Row> ds = sparkSession.readStream()
      .schema(schema)
      .format(format)
      .options(options)
      .load();

    try {
      AtomicReference<Dataset<Row>> output = new AtomicReference<>();
      ds.writeStream()
        .outputMode(OutputMode.Update())
        .options(options)
        .foreachBatch((batch, ts) -> {
          output.set(batch);
        })
        .trigger(Trigger.Once())
        .start()
        .awaitTermination();

      return Pair.of(Option.ofNullable(output.get()), String.valueOf(System.currentTimeMillis()));
    } catch (StreamingQueryException e) {
      return Pair.of(Option.empty(), String.valueOf(System.currentTimeMillis()));
    }
  }
}

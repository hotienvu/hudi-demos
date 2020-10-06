package com.vho.hudisparkstreaming;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.spark.sql.SaveMode;

import java.util.Properties;

public class HoodieConfig {
  private String preCombineFields;
  private String tableName;
  private HoodieTableType tableType;
  private String recordKeyField;
  private String partitionFields;
  private String keyGenerator;
  private String hivePartitionFields;
  private String hivePartitionExtractorClass;
  private String checkpointLocation;
  private SaveMode saveMode;
  private String basePath;
  private boolean enableHiveSync;
  private boolean hiveStylePartitioning;
  private Properties properties;

  private HoodieConfig() {
    tableName = "hoodie_table";
    partitionFields = "partition";
    hivePartitionFields = "partition";
    tableType = HoodieTableType.COPY_ON_WRITE;
    hivePartitionExtractorClass = MultiPartKeysValueExtractor.class.getCanonicalName();
    keyGenerator = CustomKeyGenerator.class.getCanonicalName();
    preCombineFields = "ts";
    enableHiveSync = false;
    hiveStylePartitioning = true;
    basePath = "/tmp/hoodie/" + tableName;
    checkpointLocation = basePath + "/.checkpoint";
  }

  public static class Builder {
    private HoodieConfig config;

    public Builder() {
      this.config = new HoodieConfig();
    }

    public Builder enableHiveSync(boolean enableHiveSync) {
      config.enableHiveSync = enableHiveSync;
      return this;
    }
    public Builder hiveStylePartitioning(boolean hiveStylePartitioning) {
      config.hiveStylePartitioning = hiveStylePartitioning;
      return this;
    }

    public Builder preCombineFields(String preCombineFields) {
      config.preCombineFields = preCombineFields;
      return this;
    }

    public Builder tableName(String tableName) {
      config.tableName = tableName;
      return this;
    }

    public Builder tableType(HoodieTableType tableType) {
      config.tableType = tableType;
      return this;
    }

    public Builder partitionFields(String partitionFields) {
      config.partitionFields = partitionFields;
      return this;
    }

    public Builder recordKeyField(String recordKeyField) {
      config.recordKeyField = recordKeyField;
      return this;
    }

    public Builder keyGenerator(String keyGenerator) {
      config.keyGenerator = keyGenerator;
      return this;
    }

    public Builder hivePartitionFields(String hivePartitionFields) {
      config.hivePartitionFields = hivePartitionFields;
      return this;
    }

    public Builder hivePartitionExtractorClass(String hivePartitionExtractorClass) {
      config.hivePartitionExtractorClass = hivePartitionExtractorClass;
      return this;
    }

    public Builder checkpointLocation(String checkpointLocation) {
      config.checkpointLocation = checkpointLocation;
      return this;
    }

    public Builder saveMode(SaveMode saveMode) {
      config.saveMode = saveMode;
      return this;
    }

    public Builder basePath(String basePath) {
      config.basePath = basePath;
      config.checkpointLocation = basePath + "/.checkpoint";
      return this;
    }

    public Builder properties(Properties properties) {
      config.properties = properties;
      return this;
    }

    public HoodieConfig build() {
      return config;
    }
  }

  /**
   * return the builder object for Hoodie Config
   */
  public static Builder builder() {
    return new Builder();
  }

  public boolean isEnableHiveSync() {
    return enableHiveSync;
  }

  public String getPreCombineFields() {
    return preCombineFields;
  }

  public String getTableName() {
    return tableName;
  }

  public HoodieTableType getTableType() {
    return tableType;
  }

  public String getPartitionFields() {
    return partitionFields;
  }

  public String getKeyGenerator() {
    return keyGenerator;
  }

  public String getHivePartitionFields() {
    return hivePartitionFields;
  }

  public String getHivePartitionExtractorClass() {
    return hivePartitionExtractorClass;
  }

  public String getCheckpointLocation() {
    return checkpointLocation;
  }

  public SaveMode getSaveMode() {
    return saveMode;
  }

  public String getBasePath() {
    return basePath;
  }

  public Properties getProperties() {
    return properties;
  }

  public String getRecordKeyField() {
    return recordKeyField;
  }

  public boolean isHiveStylePartitioning() {
    return hiveStylePartitioning;
  }
}

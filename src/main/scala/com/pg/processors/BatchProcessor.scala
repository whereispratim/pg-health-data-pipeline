package com.pg.processors


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.pg.config.AppConfig
import com.pg.storage.IcebergWriter
import com.pg.utils.LoggingUtils

object BatchProcessor {
  private val logger = LoggingUtils.getLogger(this.getClass.getName)

  def processBatch(spark: SparkSession): Unit = {
    val spark = SparkSession.builder()
      .appName("PatientDataBatchProcessor")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg.type", "hadoop") // Hadoop, Hive, or AWS Glue
      .config("spark.sql.catalog.iceberg.warehouse", AppConfig.icebergWarehousePath)
      .getOrCreate()

    // Read data from Delta Lake
    val deltaData = spark.read.format("delta").load(AppConfig.deltaTablePath)

    // Perform batch analytics
    val analytics = deltaData.groupBy("patientId")
      .agg(
        avg("heartRate").as("avgHeartRate"),
        avg("temperature").as("avgTemperature"),
        count("*").as("measurementCount")
      )

    // Write results to Iceberg
    IcebergWriter.writeToIceberg(analytics)

    logger.info("Batch processing completed successfully")
  }
}
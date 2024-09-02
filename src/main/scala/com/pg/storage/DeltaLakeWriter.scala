package com.pg.storage

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.pg.config.AppConfig
import com.pg.utils.LoggingUtils

object DeltaLakeWriter {
  private val logger = LoggingUtils.getLogger(this.getClass.getName)

  def createTables(spark: SparkSession): Unit = {
    logger.info("Creating Delta tables if they don't exist")

    // Create the patients table
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS delta.`${AppConfig.deltaTablePath}/${AppConfig.deltaPatientsTable}` (
        patientId STRING,
        timestamp TIMESTAMP,
        heartRate FLOAT,
        bloodPressure STRING,
        temperature FLOAT
      )
      USING delta
    """)

    // Create the anomaly patients table
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS delta.`${AppConfig.deltaTablePath}/${AppConfig.deltaAnomalyPatientsTable}` (
        patientId STRING,
        timestamp TIMESTAMP,
        heartRate FLOAT,
        bloodPressure STRING,
        temperature FLOAT
      )
      USING delta
    """)

    logger.info("Delta tables created successfully")
  }

  def writeToDeltalake(df: DataFrame, tableName: String, mode: SaveMode = SaveMode.Append): Unit = {
    val path = s"${AppConfig.deltaTablePath}/$tableName"

    df.write
      .format("delta")
      .mode(mode)
      .option("mergeSchema", "true")
      .saveAsTable(s"delta.`$path`")
  }
}
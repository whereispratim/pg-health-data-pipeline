package com.pg.databricks

import org.apache.spark.sql.SparkSession
import com.pg.config.AppConfig
import com.pg.utils.LoggingUtils

/**
 * Further Data Analytics
 */

object DatabricksJob {
  private val logger = LoggingUtils.getLogger(this.getClass.getName)

  def runJob(spark: SparkSession): Unit = {
    logger.info("Running Databricks job")

    // Load data from Delta Lake
    val deltaData = spark.read.format("delta").load(AppConfig.deltaTablePath)

    // Print the schema to understand the available columns
    deltaData.printSchema()

    // Perform some analytics or transformations
    // Assuming 'patientId' is a valid column in your schema
    val result = deltaData.groupBy("patientId").count()

    // Write results back to Delta Lake or another storage
    result.write.format("delta").mode("overwrite").save(AppConfig.deltaTablePath + "/analytics_results")

    logger.info("Databricks job completed successfully")
  }
}
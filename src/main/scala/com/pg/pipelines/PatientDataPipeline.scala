package com.pg.pipelines

import org.apache.spark.sql.SparkSession
import com.pg.config.AppConfig
import com.pg.databricks.DatabricksJob
import com.pg.ingestion.PatientDataProducer
import com.pg.processors.{BatchProcessor, StreamProcessor}
import com.pg.storage.DeltaLakeWriter
import com.pg.utils.LoggingUtils

object PatientDataPipeline {
  private val logger = LoggingUtils.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Starting Patient Data Pipeline")

    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("PatientDataPipeline")
      .master(AppConfig.sparkMaster)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    try {
      // Create Delta tables
      DeltaLakeWriter.createTables(spark)

      // Start Kafka Producer in a separate thread
      val producerThread = new Thread {
        override def run(): Unit = {
          PatientDataProducer.main(Array.empty)
        }
      }
      producerThread.start()

      // Start Stream Processing
      StreamProcessor.processStream(spark)

      // Run Batch Processing periodically
      while (true) {
        Thread.sleep(60000) // Wait for 1 minute
        BatchProcessor.processBatch(spark)
      }

      // Run Databricks Job periodically
      while (true) {
        Thread.sleep(3600000) // Run every hour
        DatabricksJob.runJob(spark)
      }
    } catch {
      case e: Exception =>
        logger.error("Error in Patient Data Pipeline", e)
    } finally {
      spark.stop()
    }
  }
}
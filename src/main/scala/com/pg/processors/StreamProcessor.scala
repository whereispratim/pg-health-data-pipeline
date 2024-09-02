package com.pg.processors

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import com.pg.config.AppConfig
import com.pg.storage.DeltaLakeWriter
import com.pg.utils.{SecurityUtils, LoggingUtils}
import com.pg.models.PatientData

object StreamProcessor {
  private val logger = LoggingUtils.getLogger(this.getClass.getName)

  def processStream(spark: SparkSession): Unit = {
    logger.info("Starting stream processing")

    import spark.implicits._

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConfig.kafkaBootstrapServers)
      .option("subscribe", AppConfig.kafkaTopic)
      .load()

    val patientData = kafkaStream.selectExpr("CAST(value AS STRING)")
      .as[String]
      .map { encryptedData =>
        val decryptedData = SecurityUtils.decryptPatientData(encryptedData)
        PatientData.fromJson(decryptedData)
      }
      .select(
        $"patientId",
        from_unixtime($"timestamp" / 1000).as("timestamp"),
        $"heartRate",
        $"bloodPressure",
        $"temperature"
      )

    // Define anomaly detection logic
    val anomalyCondition =
      col("heartRate") < 70 || col("heartRate") > 100 ||
        col("bloodPressure").like("%high%") || col("bloodPressure").like("%low%") ||
        col("temperature") < 35.0 || col("temperature") > 38.0

    val query = patientData.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        logger.info(s"Processing batch $batchId")

        // Write all patient data to Delta Lake
        DeltaLakeWriter.writeToDeltalake(batchDF, AppConfig.deltaPatientsTable)

        // Filter and write anomalous patient data to a separate Delta table
        val anomalousDf = batchDF.filter(anomalyCondition)
        if (anomalousDf.count() > 0) {
          logger.warn(s"Detected ${anomalousDf.count()} anomalies in batch $batchId")
          DeltaLakeWriter.writeToDeltalake(anomalousDf, AppConfig.deltaAnomalyPatientsTable)
        }
      }
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}
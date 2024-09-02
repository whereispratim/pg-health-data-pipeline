package com.pg.ingestion

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.pg.models.PatientData
import com.pg.config.AppConfig
import com.pg.utils.{SecurityUtils, LoggingUtils}
import java.util.Properties
import scala.util.Random

object PatientDataProducer {
  private val logger = LoggingUtils.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", AppConfig.kafkaBootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val topic = AppConfig.kafkaTopic

    try {
      while (true) {
        val patientData = generatePatientData()
        val jsonData = PatientData.toJson(patientData)  // Serialize to JSON
        val encryptedData = SecurityUtils.encryptPatientData(jsonData)  // Encrypt the JSON string
        val record = new ProducerRecord[String, String](topic, patientData.patientId, encryptedData)
        producer.send(record)
        logger.info(s"Sent patient data for ${patientData.patientId}")
        Thread.sleep(1000)
      }
    } catch {
      case e: Exception => logger.error("Error in patient data production", e)
    } finally {
      producer.close()
    }
  }

  private def generatePatientData(): PatientData = {
    PatientData(
      patientId = Random.alphanumeric.take(10).mkString,
      timestamp = System.currentTimeMillis(),
      heartRate = Random.nextInt(40) + 60,
      bloodPressure = s"${Random.nextInt(40) + 100}/${Random.nextInt(20) + 60}",
      temperature = 36.1 + Random.nextDouble() * 2
    )
  }
}
package com.pg.config

import com.typesafe.config.ConfigFactory

object AppConfig {
  private val config = ConfigFactory.load()

  // Kafka configuration
  val kafkaBootstrapServers: String = config.getString("kafka.bootstrap.servers")
  val kafkaTopic: String = config.getString("kafka.topic")

  // Spark configuration
  val sparkMaster: String = config.getString("spark.master")

  // Delta Lake configuration
  val deltaTablePath: String = config.getString("delta.table.path")
  val deltaPatientsTable: String = config.getString("delta.patients.table")
  val deltaAnomalyPatientsTable: String = config.getString("delta.anomaly.patients.table")

  // Iceberg configuration
  val icebergWarehousePath: String = config.getString("iceberg.warehouse.path")

  // MongoDB configuration
  val mongoUri: String = config.getString("mongo.uri")
  val mongoDatabase: String = config.getString("mongo.database")
  val mongoCollection: String = config.getString("mongo.collection")
}

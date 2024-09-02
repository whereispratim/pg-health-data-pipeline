# PG Health Data Pipeline

## Overview

The PG Health Data Pipeline is a scalable `Real-time Patient Monitoring and Analytics System` designed to ingest, process, and store real-time sensor data from healthcare devices. The system leverages modern data processing technologies, including Apache Kafka, Apache Spark, Delta Lake, Apache Iceberg, MongoDB, and Databricks, to provide real-time analytics and long-term storage solutions.

## Project Structure

- **config/**: Contains configuration files and classes.
- **models/**: Defines data models and schemas.
- **ingestion/**: Simulates real-time data production to Kafka.
- **processors/**: Includes stream and batch processing logic.
- **storage/**: Manages data storage using Delta Lake and Iceberg.
- **utils/**: Utility classes for logging and security.
- **pipelines/**: Serves as the entry point for our data pipeline and trigger each component

## Usage
- **Data Ingestion**: The pipeline reads real-time sensor data from Kafka topics. The `PatientDataProducer` simulates medical devices sending real-time patient data (heart rate, blood pressure, temperature) to a Kafka topic. In a real scenario, this would be actual medical devices in hospitals or even wearable devices used by patients at home.
- **Stream Processing**: The `StreamProcessor` consumes data from Kafka in real-time, decrypts it, and processes it using Spark Streaming. This allows for immediate analysis of patient vitals, potentially triggering alerts for medical staff if any metrics fall outside of normal ranges.
- **Short-term Storage**: Processed data is written to `Delta Lake`, providing a queryable data store for recent patient data. This allows healthcare providers to quickly access and analyze a patient's recent history.
- **Batch Processing**: The `BatchProcessor` periodically runs analytics on the accumulated data in Delta Lake, computing aggregates and trends. This could be used to identify long-term health trends, assess the effectiveness of treatments, or predict potential health issues.
- **Long-term Storage**: Aggregated data and analytics results are stored in `Iceberg` tables, providing an efficient solution for long-term data retention and analysis.

## Prerequisites

- Java 8 or higher
- Scala 2.12
- Apache Spark 3.2.0
- Apache Kafka
- Delta Lake 1.0.0
- Apache Iceberg 0.13.1
- MongoDB
- Databricks account
- Maven 3.6 or higher
- A running Hadoop or compatible file system for Iceberg

Note :
> Local or cluster setup is not covered here.

## Setup Instructions

1. **Clone the Repository**

   ```bash
   git clone https://github.com/whereispratim/pg-health-data-pipeline
   cd pg-health-data-pipeline

2. **Configure Application Settings**

   Update the application.conf file in the src/main/resources/ directory with your specific configuration, such as Kafka brokers, Delta Lake paths, etc.

3. **Build the Project**

   ```bash
   mvn clean package
   
4. **Run the Pipeline Locally**
   ```bash
   java -cp target/pg-health-data-pipeline-1.0-SNAPSHOT.jar com.pg.pipelines.PatientDataPipeline
5. **Run on Databricks**

- Upload the generated JAR to Databricks.
- Create a Databricks job to run `com.pg.databricks.DatabricksJob`.

# In progress ..

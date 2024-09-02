package com.pg
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import com.pg.config.AppConfig

class DataQualityTests extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("DataQualityTests")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("Valid heart rate values") {
    val df = spark.read.format("delta").load(s"${AppConfig.deltaTablePath}/${AppConfig.deltaPatientsTable}")
    val invalidHeartRateCount = df.filter(col("heartRate") < 0 || col("heartRate") > 220 || col("heartRate").isNull).count()
    assert(invalidHeartRateCount == 0, s"Found $invalidHeartRateCount invalid heart rate values")
  }

  test("Valid blood pressure values") {
    val df = spark.read.format("delta").load(s"${AppConfig.deltaTablePath}/${AppConfig.deltaPatientsTable}")
    val validBloodPressure = df.filter(col("bloodPressure").isin("normal", "high", "low")).count()
    val totalRecords = df.count()
    assert(validBloodPressure == totalRecords, s"Found ${totalRecords - validBloodPressure} invalid blood pressure values")
  }

  test("No duplicate patient records") {
    val df = spark.read.format("delta").load(s"${AppConfig.deltaTablePath}/${AppConfig.deltaPatientsTable}")
    val duplicateCount = df.groupBy("patientId", "timestamp")
      .count()
      .filter(col("count") > 1)
      .count()
    assert(duplicateCount == 0, s"Found $duplicateCount duplicate patient records")
  }
}
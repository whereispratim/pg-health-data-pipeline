package com.pg.storage

import org.apache.spark.sql.{DataFrame, SaveMode}
import com.pg.config.AppConfig

object IcebergWriter {
  def writeToIceberg(df: DataFrame, mode: SaveMode = SaveMode.Append): Unit = {
    df.write
      .format("iceberg")
      .mode(mode)
      .save(s"${AppConfig.icebergWarehousePath}/patient_data")
  }
}
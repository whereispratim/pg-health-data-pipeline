package com.pg.models
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
case class PatientData(
                        patientId: String,
                        timestamp: Long,
                        heartRate: Int,
                        bloodPressure: String,
                        temperature: Double
                      )

object PatientData {
  implicit val formats = DefaultFormats

  def toJson(data: PatientData): String = {
    Serialization.write(data)
  }

  def fromJson(json: String): PatientData = {
    Serialization.read[PatientData](json)
  }
}
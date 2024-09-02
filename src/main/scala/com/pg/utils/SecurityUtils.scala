package com.pg.utils

object SecurityUtils {
  def encryptPatientData(data: String): String = {
    // Implement encryption logic here
    s"encrypted_$data"
  }

  def decryptPatientData(encryptedData: String): String = {
    // Implement decryption logic here
    encryptedData.replace("encrypted_", "")
  }
}
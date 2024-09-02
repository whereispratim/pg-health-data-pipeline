package com.pg.utils

import org.apache.log4j.Logger

object LoggingUtils {
  def getLogger(name: String): Logger = {
    Logger.getLogger(name)
  }

  /*def logError(message: String, throwable: Throwable): Unit = {
    logger.error(message, throwable)
  }*/
}

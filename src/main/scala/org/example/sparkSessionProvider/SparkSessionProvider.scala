package org.example.sparkSessionProvider

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {

  def createSparkSession(appName: String): SparkSession = {
    SparkSession.builder().master("local")
      .appName(appName)
      .getOrCreate()
  }

}

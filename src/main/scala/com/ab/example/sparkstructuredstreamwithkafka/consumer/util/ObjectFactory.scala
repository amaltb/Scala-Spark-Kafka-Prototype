package com.ab.example.sparkstructuredstreamwithkafka.consumer.util

import org.apache.spark.sql.SparkSession

object ObjectFactory {

  /**
    * Method to acquire active spark session
    * @param applicationName
    * @param master
    * @return
    */
  def getOrCreateSparkSession(applicationName: String, master: String = ""): SparkSession = {

    val spark = SparkSession.builder().appName(applicationName).config("spark.scheduler.mode", "FAIR")
    if(!master.isEmpty)
      spark.master(master)

    spark.getOrCreate()
  }

}

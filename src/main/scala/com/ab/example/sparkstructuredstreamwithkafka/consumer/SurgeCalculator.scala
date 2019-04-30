package com.ab.example.sparkstructuredstreamwithkafka.consumer

import com.ab.example.sparkstructuredstreamwithkafka.consumer.util.ConsumerUtils.parseCommandLineArgs
import com.ab.example.sparkstructuredstreamwithkafka.consumer.util.{Constants, ConsumerUtils, ObjectFactory, UDFWrapper}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{expr, from_json, window}
import org.apache.spark.sql.streaming.Trigger

import scala.util.parsing.json.JSON

/**
  * Surge calculator app consumes pre-processed supply & demand requests from kafka and calculate surge ratio for each
  * zip-code in a given batch interval.
  */
object SurgeCalculator {

  /**
    * method to parse pre-processed json string to df columns and add time window based on the window interval string in
    * the json.
    *
    * @param spark
    * @param df
    * @return
    */
  def denormalise_df(spark: SparkSession, df: DataFrame):DataFrame = {
    import spark.implicits._
    val value_df = df.selectExpr("CAST(value AS STRING)").as[String].toDF()

    val json_schema = spark.read.json("/Users/ambabu/Documents/PersonalDocuments/code-samples/" +
      "SparkStructuredStreamWithKafka/src/main/resources/intermediate_data.json").schema

    val parsed_df = value_df.select(from_json($"value", json_schema).as("data")).select("data.*")

    val window_fn = (window_ts: String) => UDFWrapper.extract_ts(window_ts)
    val window_udf = spark.udf.register("window_udf", window_fn)

    parsed_df.withColumn("timestamp", window_udf(parsed_df.col("window"))).drop("window")
  }

  /**
    * method to calculate surge ratio for each zip-code in the given window interval.
    *
    * @param spark
    * @param demand_supply_df
    * @return
    */
  def calculate_surge_ratio(spark: SparkSession, demand_supply_df: DataFrame): DataFrame = {
    val surge_fval = (zip_code: String, d_count: String, s_count: String) => UDFWrapper.findSurgeRatio(zip_code, d_count, s_count)

    val surge_udf = spark.udf.register("surge_udf", surge_fval)

    demand_supply_df.withColumn("surge_ratio", surge_udf(demand_supply_df.col("d_zip_code"),
      demand_supply_df.col("d_count"), demand_supply_df.col("s_count")))
  }

  /**
    * application run method.
    *
    * @param args
    * @param spark
    */
  def run(args: Array[String], spark: SparkSession): Unit = {
    import spark.implicits._
    // defining all required arguments for this application
    val reqArgs = Map("kafkaParams" -> "Kafka Parameters JSON string")

    try{
      val cmdArgs = parseCommandLineArgs(args, reqArgs)
      val kafkaParameterMap = JSON.parseFull(cmdArgs("kafkaParams")).get.asInstanceOf[Map[String, String]]

      val demand_df = ConsumerUtils.subscribeKafkaStream(spark, kafkaParameterMap, Constants.PROCESSED_DEMAND_TOPIC)
      val supply_df = ConsumerUtils.subscribeKafkaStream(spark, kafkaParameterMap, Constants.PROCESSED_SUPPLY_TOPIC)

      val denorm_demand_df = denormalise_df(spark, demand_df).filter("zip_code != ''")
        .withColumnRenamed("count", "d_count")
        .withColumnRenamed("zip_code", "d_zip_code")
        .withColumn("d_window", window($"timestamp", "10 seconds"))

      val denorm_supply_df = denormalise_df(spark, supply_df).filter("zip_code != ''")
        .withColumnRenamed("zip_code", "s_zip_code")
        .withColumnRenamed("count", "s_count")
        .withColumn("s_window", window($"timestamp", "10 seconds"))

      val demand_supply_df = denorm_demand_df.join(denorm_supply_df,
        expr(
          """
            |d_zip_code = s_zip_code AND
            |d_window = s_window
          """.stripMargin))

      val df_with_surge_ratio = calculate_surge_ratio(spark, demand_supply_df)

      val console_query = df_with_surge_ratio
        .writeStream
        .format("console")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .outputMode("append")
        .start()

      console_query.awaitTermination()
    }
  }

  /**
    * application entry point.
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = ObjectFactory.getOrCreateSparkSession("SurgeCalculator", "local")
    run(args, spark)
  }
}

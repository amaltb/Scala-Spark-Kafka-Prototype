package com.ab.example.sparkstructuredstreamwithkafka.consumer

import com.ab.example.sparkstructuredstreamwithkafka.consumer.util.ConsumerUtils.parseCommandLineArgs
import com.ab.example.sparkstructuredstreamwithkafka.consumer.util.{Constants, ConsumerUtils, ObjectFactory, UDFWrapper}
import com.ab.example.sparkstructuredstreamwithkafka.producer.util.LogFactory
import org.apache.log4j.Level
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{array, lit, window}
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.util.parsing.json.JSON

/**
  * StructuredStreamProcessor app reads the raw csv supply & demand data from kafka and process them to generate
  * intermediate json data, which is then written back to kafka.
  */
object StructuredStreamProcessor {

  private val log = LogFactory.getLogger("./log/consumer.log", Level.DEBUG)

  /**
    * method to de-serialize and parse car demand structured stream. This also involve de-normalizing the value data in
    * kafka df
    *
    * @param spark
    * @param df
    * @return
    */
  def denormalise_df(spark: SparkSession, df: DataFrame, columnMap: Map[String, Int],
                     zipCodeBroadcastVar: Broadcast[collection.mutable.Map[String, ((String, String), (String, String))]]): DataFrame = {
    import spark.implicits._
    val value_df = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)].toDF()

    val udf_f = (col_val:String, col_pos: Int) => UDFWrapper.extract_column_value(col_val, col_pos)
    val col_udf = spark.udf.register("col_parse_udf", udf_f)

    val denorm_df = columnMap.foldLeft(value_df)((acc, ele) => acc.withColumn(ele._1, col_udf(acc.col("value"),
      lit(ele._2)))).drop("value")

    val udf_f1 = (lat: String, long: String) => UDFWrapper.lookupZipCode(lat, long, zipCodeBroadcastVar)
    val zip_code_udf = spark.udf.register("zip_code_lookup_udf", udf_f1)

    denorm_df.withColumn("zip_code", zip_code_udf(denorm_df.col("lat"), denorm_df.col("long")))
      .drop("lat").drop("long")
  }


  /**
    * prepare_kafka_df method to add mandatory value column to processed dataframe, so that it can be persisted to kafka.
    *
    * @param spark
    * @param df
    * @return
    */
  def prepare_kafka_df(spark:SparkSession, df: Dataset[Row]): DataFrame = {

    val columnNames = df.columns.toSeq

    val jsonifier = (values: Seq[String]) => UDFWrapper.jsonifier(values, columnNames)
    val column_udf = spark.udf.register("jsonifier", jsonifier)

    df.withColumn("value", column_udf(array(columnNames.head, columnNames.tail: _*)))
  }

  /** This method does
    * Parse command line arguments
    * Subscribe to kafka streams
    * Call location udf to add location unit
    * Merge both demand and supply datasets on location unit
    * Calculate the demand supply ratio for each of the location unit and apply surge pricing algorithm on the ratio
    *
    * @param args
    */
  def run(args: Array[String], spark: SparkSession): Unit = {
    import spark.implicits._
    // defining all required arguments for this application
    val reqArgs = Map("kafkaParams" -> "Kafka Parameters JSON string")

    try {
      val cmdArgs = parseCommandLineArgs(args, reqArgs)
      val kafkaParameterMap = JSON.parseFull(cmdArgs("kafkaParams")).get.asInstanceOf[Map[String, String]]

      val demand_df = ConsumerUtils.subscribeKafkaStream(spark, kafkaParameterMap, Constants.DEMAND_TOPIC)

      val supply_df = ConsumerUtils.subscribeKafkaStream(spark, kafkaParameterMap, Constants.SUPPLY_TOPIC)

      val zipCodeLookupMap = ConsumerUtils.getZipCodeMap()
      val zipCodeMapBrodcastVar = spark.sparkContext.broadcast(zipCodeLookupMap)

      val denorm_demand_df = denormalise_df(spark, demand_df, Map("customer_id" -> 0, "lat" -> 1, "long" -> 2),
        zipCodeMapBrodcastVar).selectExpr("CAST(zip_code as STRING)", "CAST(customer_id as BIGINT)",
        "CAST(timestamp AS TIMESTAMP)").as[(String, BigInt, Timestamp)]


      val denorm_supply_df = denormalise_df(spark, supply_df, Map("car_id" -> 0, "lat" -> 1, "long" -> 2),
        zipCodeMapBrodcastVar).selectExpr("CAST(zip_code as STRING)", "CAST(car_id as BIGINT)",
        "CAST(timestamp AS TIMESTAMP)").as[(String, BigInt, Timestamp)]

      /*
      Wanted to do stateless processing of both cab demand and supply data. So using below approach to do stateless
      processing of cab demand/ supply data of current micro batch alone without considering the previous batches. Spark
      Structured Stream Processor does not have a built in API to do this till Spark version 2.4. This output would be
      later written to another kafka topic for further processing.
       */
      val groupedDemandCount = denorm_demand_df.
        groupByKey{
          case (zip_code, id, ts) => zip_code
        }.mapGroupsWithState(
        (zip: String, tuples: Iterator[(String, BigInt, Timestamp)], value: GroupState[Int]) =>
        {
          val tup_list = tuples.toList
          (zip, tup_list.size, tup_list.head._3)
        }
      ).toDF("zip_code", "count", "timestamp")
        .selectExpr("cast(zip_code as STRING)", "cast(count as STRING)", "cast(timestamp as TIMESTAMP)")
        .withColumn("window", window($"timestamp", "10 seconds"))
        .drop("timestamp")
        .selectExpr("cast(zip_code as STRING)", "cast(count as STRING)", "cast(window as STRING)")


      val groupedSupplyCount = denorm_supply_df.
        groupByKey{
          case (zip_code, id, ts) => zip_code
        }.mapGroupsWithState((zip: String, tuples: Iterator[(String, BigInt, Timestamp)], value: GroupState[Int]) => {
        val tup_list = tuples.toList
        (zip, tup_list.size, tup_list.head._3)
      }).toDF("zip_code", "count", "timestamp")
        .selectExpr("cast(zip_code as STRING)", "cast(count as STRING)", "cast(timestamp as TIMESTAMP)")
        .withColumn("window", window($"timestamp", "10 seconds"))
        .drop("timestamp")
        .selectExpr("cast(zip_code as STRING)", "cast(count as STRING)", "cast(window as STRING)")

      /*
      Need to write the intermediate processed data to kafka topic, because SSP does not allow multiple aggregations on
      streaming dataset. As I need to get a per zip code cab demand and request followed by a join on zipcode and window
      to figure out the Surge Pricing ratio for that zip location in the given window, this will lead to multiple aggregation
      on the resulting dataset lineage. Hence persisting the aggregated data back to kafka and figure out surge ratio from
      another spark app.
       */
      val demand_query = ConsumerUtils.writeToKafkaStream(prepare_kafka_df(spark, groupedDemandCount.toDF()),
        kafkaParameterMap, "/Users/ambabu/Documents/PersonalDocuments/code-samples/" +
          "SparkStructuredStreamWithKafka/spark-checkpoint/demandstream", Constants.PROCESSED_DEMAND_TOPIC)

      val supply_query = ConsumerUtils.writeToKafkaStream(prepare_kafka_df(spark, groupedSupplyCount.toDF()),
        kafkaParameterMap, "/Users/ambabu/Documents/PersonalDocuments/code-samples/" +
          "SparkStructuredStreamWithKafka/spark-checkpoint/supplystream", Constants.PROCESSED_SUPPLY_TOPIC)

      spark.streams.awaitAnyTermination()

    } catch {
      case e1: java.util.NoSuchElementException => log.error("Unable to parse kafka parameters from command line arguments. \nDetails: " + e1)
      case e2: RuntimeException => log.error("Exiting application due to unknown exception: \nDetails: " + e2)
    } finally {
      spark.close()
    }
  }

  /**
    * Entry point to the application
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = ObjectFactory.getOrCreateSparkSession("StructuredStreamProcessor", "local[4]")
    run(args, spark)
  }
}
package com.ab.example.sparkstructuredstreamwithkafka.consumer.util

import com.ab.example.sparkstructuredstreamwithkafka.producer.util.LogFactory
import org.apache.commons.cli._
import org.apache.log4j.Level
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object ConsumerUtils {

  private val log = LogFactory.getLogger("./log/consumer.log", Level.DEBUG)

  /**
    * Utility method to parse required and optional command line arguments...
    *
    * @param args
    * @param reqArgs
    * @param optionalArgs
    * @return
    */
  def parseCommandLineArgs(args: Array[String], reqArgs: Map[String, String],
                           optionalArgs: Map[String, String] = null): collection.mutable.Map[String, String] =
  {
    val options = new Options()

    for (ele <- reqArgs)
      {
        val arg = new Option(ele._1, true, ele._2)
        arg.setRequired(true)
        options.addOption(arg)
      }

    if(optionalArgs != null)
      {
        for (ele <- optionalArgs)
        {
          val arg = new Option(ele._1, true, ele._2)
          arg.setRequired(true)
          arg.setOptionalArg(true)
          options.addOption(arg)
        }
      }

    val parser = new DefaultParser()
    val formatter = new HelpFormatter()
    var cmd: CommandLine = null

    try{
      cmd = parser.parse(options, args)
    } catch {
      case p: ParseException => {
        formatter.printHelp("Stream processor utility", options)
        throw new RuntimeException("Could not parse command line arguments. \nDetails: " + p)
      }
    }

    var commandMap = collection.mutable.Map[String, String]()

    for (ele <- reqArgs)
    {
      commandMap += (ele._1 -> cmd.getOptionValue(ele._1))
    }

    if(optionalArgs != null)
      {
        for (ele <- optionalArgs)
        {
          commandMap += (ele._1 -> (if (cmd.getOptionValue(ele._1) == null) "ND" else cmd.getOptionValue(ele._1)))
        }
      }

    commandMap
  }

  /**
    * method to subscribe to an active stream from a given kafka topic.
    *
    * @param spark
    * @param KafkaParams
    * @param topic
    * @return
    */
  def subscribeKafkaStream(spark: SparkSession, KafkaParams: Map[String, String], topic: String): DataFrame = {
    try {
      val df = spark.read
        .format("kafka")
        .options(KafkaParams)
        .option("failOnDataLoss", "false")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
      df
    } catch {
      case e: Exception => throw new RuntimeException(String.format("Could not subscribe kafka topic: %s due to " +
        "exception. \n Details: %s", topic, e))
    }
  }


  /**
    * method to persist the dataframe to kafka topic.
    *
    * @param df
    * @param kafkaParams
    * @param checkpointDir
    * @return
    */
  def writeToKafkaStream(df: DataFrame, kafkaParams: Map[String, String], checkpointDir: String, topic: String): StreamingQuery =
  {
    log.info("Persisting intermediary dataframe to kafka... ")

    try{
      df.selectExpr("CAST(value AS STRING)")
        .writeStream
        .format("kafka")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .outputMode("update")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("checkpointLocation", checkpointDir)
        .option("topic", topic)
        .start()
    } catch {
      case e: Exception => throw new RuntimeException(String.format("Could not write to kafka due to " +
        "exception. \n Details: %s", e))
    }
  }

  /**
    * function to create zip code lookup map, from geo code csv file.
    *
    * @return
    */
  def getZipCodeMap(): collection.mutable.Map[String, ((String, String), (String, String))] = {
    var zipCodeMap = collection.mutable.Map[String, ((String, String), (String, String))]()

    val zipCodeSource = Source.fromFile(Constants.ZIP_CODE_LOOKUP_FILE_NAME)

    for (line <- zipCodeSource.getLines() if !line.startsWith("zip_code") if line.split(",").length == 5)
    {
      val items = line.split(",")
      zipCodeMap += items(0) -> ((items(1), items(2)), (items(3), items(4)))
    }
    zipCodeMap
  }
}

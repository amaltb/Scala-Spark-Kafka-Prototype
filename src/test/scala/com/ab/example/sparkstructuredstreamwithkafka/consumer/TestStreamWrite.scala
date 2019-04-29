package com.ab.example.sparkstructuredstreamwithkafka.consumer

import com.ab.example.sparkstructuredstreamwithkafka.consumer.util.UDFWrapper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{array, window}
import org.apache.spark.sql.streaming.{GroupState, Trigger}


object TestStreamWrite {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[4]").setAppName("test")

    val spark = SparkSession.builder.config(conf).getOrCreate

    import spark.implicits._

    val dataset = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "start")
      .option("startingOffsets", "earliest")
      .load

    val df = dataset.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]


    val word_df = df.flatMap {
      case (value, ts) => value.toString.split(" ").map(word => (word, 1, ts))
    }

    val cnt_df = word_df.groupByKey {
      case (key, cnt, ts) => key
    }.mapGroupsWithState((str: String, tuples: Iterator[(String, Int, Timestamp)], value: GroupState[Int]) => {
      val tuple_list = tuples.toList
      (str, tuple_list.size, tuple_list.head._3)
    }).toDF("word", "count", "ts")


    val columnNames = cnt_df.columns.toSeq

    val jsonifier = (values: Seq[String]) => UDFWrapper.jsonifier(values, columnNames)
    val column_udf = spark.udf.register("jsonifier", jsonifier)

    val final_df = cnt_df.withColumn("value", column_udf(array(columnNames.head, columnNames.tail: _*)))
      .selectExpr("cast(value as STRING)", "CAST(ts AS TIMESTAMP)").as[(String, Timestamp)]


    final_df.withColumn("window", window($"ts", "10 seconds")).show(false)


//    val query = final_df
//      .writeStream
//      .format("kafka")
//      .outputMode("update")
//      .trigger(Trigger.ProcessingTime("10 seconds"))
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("checkpointLocation", "/Users/ambabu/Documents/PersonalDocuments/code-samples/SparkStructuredStreamWithKafka/spark-checkpoint/TestStreamWrite")
//      .option("topic", "end")
//      .start
//
//    query.awaitTermination()
  }
}

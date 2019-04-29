package com.ab.example.sparkstructuredstreamwithkafka.consumer.util

import com.ab.example.sparkstructuredstreamwithkafka.producer.util.LogFactory
import org.apache.log4j.Level
import org.apache.spark.broadcast.Broadcast

/**
  * UDFWrapper is a serializable wrapper object for udf functions. This is to define non-serializable bound
  * variables as transient while serializing udf function value.
  */
object UDFWrapper extends Serializable {

  /**
    * function to extract start ts from window ts string..
    * sample [2019-04-19 13:10:20, 2019-04-19 13:10:30]
    *
    * @param window_ts
    * @return
    */
  def extract_ts(window_ts: String): String = {
    val pattern = "\\[([0-9: -]+), ([0-9: -]+)\\]".r
    val pattern(window_start, window_end) = window_ts
    window_start
  }


  /**
    * function to find surge pricing ratio at a given zip_code. Currently based on demand supply ratio,
    * //TODO modify surge algorithm to factor weather(at given zip_code) as a driving metric for surge calculation
    *
    * @param zip_code
    * @param d_count
    * @param s_count
    * @return
    */
  def findSurgeRatio(zip_code: String, d_count: String, s_count: String): Double = {
    log.info("Calculating surge ratio for zipcode: %s".format(zip_code))
    try {
      val supply_count = s_count.toDouble
      val demand_count = d_count.toDouble
      if (supply_count / demand_count >= 1) 1 else 1 + (1 - supply_count / demand_count)
    } catch {
      case e: NumberFormatException =>
        log.error(("Could not calculate surge ratio at zip_code %s due to " +
          "exception. \nDetails: %s").format(zip_code, e))
        1.0
      case e1: ArithmeticException =>
        log.error(("Could not calculate surge ratio at zip_code %s due to " +
          "exception. \nDetails: %s").format(zip_code, e1))
        1.0
    }
  }

  @transient
  private val log = LogFactory.getLogger("./log/consumer.log", Level.DEBUG)

  /**
    * function to split and extract a value from a string
    *
    * @param value
    * @param pos
    * @return
    */
  def extract_column_value(value: String, pos: Int): String = {
    value.split(",")(pos)
  }

  /**
    * function to lookup zip code for a given lat and long from the broadcasted zip code map.
    *
    * @param lat
    * @param long
    * @param zipCodeBroadcastVar
    * @return
    */
  def lookupZipCode(lat: String, long: String,
                    zipCodeBroadcastVar: Broadcast[collection.mutable.Map[String, ((String, String),
                      (String, String))]]): String = {
    log.info("Looking up zip code for lat: %s lng: %s".format(lat, long))
    for (ele <- zipCodeBroadcastVar.value)
    {
      val zipCode = ele._1

      try{
        val cur_lat = (math rint lat.toDouble * 1000) / 1000
        val cur_long = (math rint long.toDouble * 1000) / 1000

        val min_lat = (math rint ele._2._1._1.toDouble * 1000) / 1000
        val max_lat = (math rint ele._2._1._2.toDouble * 1000) / 1000
        val min_long = (math rint ele._2._2._1.toDouble * 1000) / 1000
        val max_long = (math rint ele._2._2._2.toDouble * 1000) / 1000

        if((min_lat <= cur_lat && cur_lat <= max_lat) && (min_long <= cur_long && cur_long <= max_long))
        {
          log.info("Zip code for lat: %s lng: %s is %s".format(lat, long, zipCode))
          return zipCode
        }
      } catch {
        case ne: NumberFormatException => log.error(("Looking up zip code failed for lat:%s lng: %s " +
          "with exception. \nDetails: %s").format(lat, long, ne))
      }
    }
    log.info("Unable to lookup zip code for given lat:%s lng:%s".format(lat, long))
    ""
  }

  /**
    * udf to create a json string column consisting of all the other column values.
    *
    * @param values
    * @param columnNames
    * @return
    */
  def jsonifier(values: Seq[Any], columnNames: Seq[String]): String = {
    val json_string = new StringBuilder()
    json_string.append("{")
    for ((colName, colValue) <- columnNames zip values) {
      if(json_string.length > 1) json_string.append(",")
      json_string.append("\"").append(colName).append("\":\"").append(colValue.toString).append("\"")
    }
    json_string.append("}").mkString
  }
}

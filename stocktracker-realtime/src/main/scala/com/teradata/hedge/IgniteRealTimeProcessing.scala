package com.teradata.hedge

import java.{ lang, util }

import org.apache.flink.streaming.api.scala._
import IgniteConnection.IgniteSink
import flinkConnection.Flink_Connector
import org.apache.flink.api.common.typeinfo
import mail.SendMail
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.io.jdbc.{ JDBCAppendTableSink, JDBCOutputFormat }
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{ JsonNode, ObjectMapper }
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.{ Table, TableEnvironment }
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import java.sql.Types

import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import IgniteConnection.IgniteJDBC

object IgniteRealTimeProcessing {
  import org.apache.log4j._
  val LOG = Logger.getLogger(this.getClass)

  //Compare real time data and historic Ignite data
  def CompareHistoricAndRealTime(realTimeData: (String, String, Double), threshold: Double, path: String): Unit = {

    val timestamp = (realTimeData._2.replace('"', ' ').trim).split(" ")(1).substring(0,8)
    val stockName = realTimeData._1.replace('"', ' ').trim
    val sql = "select histratio from stock_transformation where symbol =" + "'" + stockName + "'" + " and timestampValue =" + "'" + timestamp + "';";
    
    val histValue: Double = Option(IgniteJDBC.getIgniteData(sql)).getOrElse(99999.0)

    if (histValue != 99999.0) {
      println("Historic Value =>" + histValue + "  Real time data =>" + realTimeData._3)

      //var ratioDeviation :Double = ((realTimeData._3-histValue)*100)/histValue
      var ratioDeviation: Double = Option(((realTimeData._3 - histValue) * 100) / histValue).getOrElse(99999.0)

      if (ratioDeviation.isNaN) ratioDeviation = 99999.0

      if (ratioDeviation.isInfinite) {
        ratioDeviation = 99999.0
      }

      if (ratioDeviation != 99999.0) {
        if (ratioDeviation > threshold) {
          AlertGeneration(realTimeData, histValue, ratioDeviation, path, threshold)
        } else {
          LOG.info("Alert is not generated!!")
          println("Alert is not generated!!")
        }
      } else {
          LOG.info("Alert is not generated!!")
          println("Alert is not generated!!")
      }
    }
  }

  //Generate alert in the form of msg and log
  def AlertGeneration(tuple: (String, String, Double), histValue: Double, deviation: Double, path: String, threshold: Double): Unit = {
    val mail = new SendMail()
    val props = mail.loadPropertiesFile(path)
    val sql = "INSERT INTO STOCK_ALERT (SYMBOL,TIMESTAMPVALUE,HISTRATIO,CURRENTRATIO,RATIODEVIATION,THRESHOLD) VALUES (?,?,?,?,?,?)"
    
    val s: String = " Alert has generated for stock " + tuple._1 + " and timestamp " +
      tuple._2 + ". Historic Ratio : " + histValue + ", Current Ratio : " + tuple._3 + ", Ratio deviation : " + deviation + ", Threshold : " + threshold
    //mail.send(props, s)
    val symbol = tuple._1.replace('"', ' ').trim
    val timeStampValue = tuple._2.replace('"', ' ').trim
    IgniteJDBC.putIgniteData(sql,symbol,timeStampValue,"%.5f".format(histValue).toDouble,tuple._3,"%.5f".format(deviation).toDouble,threshold)
    LOG.error(s)
    println(s)
  }

  //Write data into Ignite
  def writeDataIntoIgnite(data: DataStream[StockMarket]) = {

    val ig = new IgniteSink()
    data.addSink(ig)
  }

  lazy val jsonParser = new ObjectMapper()
  def parseJSON(m: String): (String, String, Double, Double) = {
    try {
      val jsonNode = jsonParser.readValue(m, classOf[JsonNode])
      //val stockName =Option(jsonNode.get("stock_symbol")).getOrElse("Invalid").asInstanceOf[String]

      (jsonNode.get("symbol").toString, jsonNode.get("timestampValue").toString, jsonNode.get("price").asDouble(), jsonNode.get("volume").asDouble())

    } catch {
      case ex: Exception =>
        println("Unable to parse the json data " + m)
        ("Invalid", "Invalid", 0.0, 0.0)
    }

  }
  def main(args: Array[String]): Unit = {

    LOG.error("=========== Log has Started ...===================================")

    val params = ParameterTool.fromArgs(args)

    if (params.getNumberOfParameters < 3) {
      println("Missing parameters!\n"
        + "Usage: Kafka --input-topic <topic> --output-topic <topic> "
        + "--bootstrap.servers <kafka brokers> "
        + "--zookeeper.connect <zk quorum> --group.id <some id> [--prefix <prefix>]")
      return
    }
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    
    env.getConfig.setGlobalJobParameters(params)

    print(params.getProperties)
 
    val kafkaConsumer = new FlinkKafkaConsumer010(
      //  "test",
      params.getRequired("input-topic"),
      new SimpleStringSchema,
      params.getProperties)
    LOG.error("connected kafka")
    val text = env
      .addSource(kafkaConsumer)


    val windowSize = params.get("windowsize").toInt //3
   
    val msg = text.map(m => parseJSON(m))
      .filter(f => !f._1.contains("Invalid"))
      .keyBy(0).countWindow(windowSize, 1) //.sum(3)
      .apply((key, window, events, out: Collector[(String, String, Double, Double, Double, Double)]) => {
        // val cnt = (events.map(m=>m._3
        out.collect(events.map(m => m._1).last, events.map(m => m._2).last, events.map(m => m._3).last, events.map(m => m._4).last, (events.map(m => m._3).sum / (events.map(m => m._3).seq.size)), (events.map(m => m._4).sum / (events.map(m => m._4).seq.size))) //(cell, window.getEnd, events.map( _._3 ).sum,events.map( _._4 ).sum ))
      })

    println("================msg===================")

    val result = msg.map(m => {
      var priceChange: Double = Option(((m._3 - m._5) * 100) / m._5).getOrElse(0.0)
      var volumeChange: Double = Option((((m._4 - m._6) * 100) / m._6)).getOrElse(0.0)
      var ratio: Double = Option((priceChange / volumeChange)).getOrElse(0.0)
      if (ratio.isNaN) ratio = 0.0
      if (priceChange.isNaN) priceChange = 0.0
      if (volumeChange.isNaN) volumeChange = 0.0
      (m._1, m._2, "%.5f".format(m._3).toDouble, "%.5f".format(m._4).toDouble,"%.5f".format(m._5).toDouble,"%.5f".format(priceChange).toDouble, "%.5f".format(m._6).toDouble, "%.5f".format(volumeChange).toDouble,"%.5f".format(ratio).toDouble)
    })

    val data = result.map(m =>
      StockMarket((m._1.replace('"', ' ').trim), (m._2.replace('"', ' ').trim), m._3, m._4, m._5, m._6, m._7, m._8, m._9))
    val threshold = params.get("threshold").toDouble
    val path = params.get("path")
    //Compare the Historic and real time data for generate alert
    writeDataIntoIgnite(data)
    val compareData = result.map(m => {

      val key = (m._1, m._2, m._9)

     // CompareHistoricAndRealTimeHashmap(key, HistoricData, threshold, path)
      CompareHistoricAndRealTime(key,threshold,path)
    })
    env.execute("RealTimeStreamingDataProcessing")
  }
}
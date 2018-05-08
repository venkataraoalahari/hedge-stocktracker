package com.teradata.hedge

import java.{lang, util}

import org.apache.flink.streaming.api.scala._
import IgniteConnection.IgniteSink
import flinkConnection.Flink_Connector
import org.apache.flink.api.common.typeinfo
import mail.SendMail
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.io.jdbc.{JDBCAppendTableSink, JDBCOutputFormat}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import java.sql.Types

import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

//import org.apache.flink.table.api.scala.Slide
//import org.apache.flink.table.sinks.CsvTableSink

/**

  * --bootstrap.servers localhost:9092 --zookeeper.connect  localhost:2181  --group.id myconsumer --input-topic test --threshold -100 --windowsize 3
  * }}}
  */


object RealTimeStreamingProcessing  extends Utils_scala {

  import org.apache.log4j._

  val LOG = Logger.getLogger(this.getClass)

  //Get  data from cassandra
  def getDatafromCassandra(sql: String): Double = {
    val obj = new Flink_Connector(sql)
    val value: Double = obj.getValue()
    obj.closeConnection()
    return value
  }

  //Compare real time data and historic cassandra data
  def CompareHistoricAndRealTime(realTimeData: (String, String, Double), threshold: Double, path: String): Unit = {

    val timestamp = (realTimeData._2.replace('"', ' ').trim).split(" ")(1)
    val stockName = realTimeData._1.replace('"', ' ').trim
    val sql = "select histratio from hedge.stock where symbol =" + "'" + stockName + "'" + " and timestampvalue =" + "'" + timestamp + "';";
    // println(sql)
    val histValue: Double = Option(getDatafromCassandra(sql)).getOrElse(99999.0)
    //println("cassandra data --> " + histValue)

    if (histValue != 99999.0){
    //println("Key is == "+ key)
      println("Historic Value =>" + histValue + "Real time data" + realTimeData._3)

    //var ratioDeviation :Double = ((realTimeData._3-histValue)*100)/histValue
    var ratioDeviation: Double = Option(((realTimeData._3 - histValue) * 100) / histValue).getOrElse(99999.0)

    if (ratioDeviation.isNaN) ratioDeviation = 99999.0

    if (ratioDeviation.isInfinite) {
      ratioDeviation = 99999.0
    }
    //println("Ratio Deviation =>" + ratioDeviation)
    // println("condition - " + ratioDeviation != 99999.0)
    if (ratioDeviation != 99999.0) {
      if (ratioDeviation > threshold) {
        AlertGeneration(realTimeData, histValue, ratioDeviation, path,threshold)
        //LOG.error("Alert is Generated ..Ratio deviation " + ratioDeviation)
        //  println("Alert is Generated ..Ratio deviation " + ratioDeviation)
      } else {
        // println("Alert not Generated ...")
        //LOG.error("Alert not Generated ..")
      }
    } else {
      // println("Alert not Generated ...99999")
      //LOG.error("Alert not Generated ..")
    }
  }
  }


  //Generate alert in the form of msg and log
  def AlertGeneration(tuple: (String, String, Double), histValue: Double, deviation: Double, path: String,threshold:Double): Unit = {
    val mail = new SendMail()
    val props = mail.loadPropertiesFile(path)
    val s: String = " Alert has generated for stock " + tuple._1 + " and timestamp " +
      tuple._2 + ". Historic Ratio : " + histValue + ", Current Ratio : " +tuple._3+", Ratio deviation : " + deviation+", Threshold : "+threshold
    //mail.send(props, s)
    LOG.error(s)
    println(s)
    ///s.wr  //writeAsText(path).setParallelism(1)
  }

  // Define case stockmarket class
  //case class StockMarket(stock_symbol:String,transactionTime:String,price:Double,volume:Double,priceAvg :Double,priceChange :Double,volumeAvg:Double,volumeChange:Double,ratio:Double)

//Write data into Ignite
  def writeDataIntoIgnite(data: DataStream[StockMarket]) = {

    val ig = new IgniteSink()
    data.addSink(ig)
  }

  //Get all data from cassandra and put it inot hashmap
  def getAllDataFromCassandra(): util.HashMap[String, lang.Double]  ={
    val sql = "select * from hedge.stock;"
    val obj = new Flink_Connector(sql)
    val HistoricData = obj.getHashMap
    import scala.collection.JavaConversions._
    System.out.println("cassandra data size " + HistoricData.size())
  //  println("===================cassandra data =======")
    /*HistoricData.keys.foreach { i =>
      print("Key = " + i)
      println(" Value = " + HistoricData(i))
    }*/
    obj.closeConnection()
    HistoricData
  }

  lazy val jsonParser = new ObjectMapper()

  //Compare real time data and historic cassandra data
  def CompareHistoricAndRealTimeHashmap(realTimeData: (String, String, Double), HistoricData: util.HashMap[String, lang.Double], threshold: Double, path: String): Unit = {
    //val key :String  = UtilityClass.concatString(realTimeData._1,realTimeData._2)
    val key: String = ConcatString(realTimeData._1, realTimeData._2)
    //val histValue:Double = Option(HistoricData.get(key)).getOrElse(0.0).asInstanceOf[Double]
    var histValue = 99999.0
   /* println("key "+ key)
    println("hashmap value "+histValue)
    println(HistoricData.containsKey(key))
*/
    if (HistoricData.containsKey(key)) {
      histValue = HistoricData.get(key)
      //  println("key exist "+key +"value "+histValue)

      //val histValue:Double = HistoricData.get(key)


      //println("Key is == "+ key)
     // println("Historic Value =>" + histValue + "Real time data" + realTimeData._3)

      //var ratioDeviation :Double = ((realTimeData._3-histValue)*100)/histValue
      var ratioDeviation: Double = Option(((realTimeData._3 - histValue) * 100) / histValue).getOrElse(99999.0)

      if (ratioDeviation.isNaN) ratioDeviation = 99999.0

      if (ratioDeviation.isInfinite) {
        ratioDeviation = 99999.0
      }
      //println("Ratio Deviation =>" + ratioDeviation)
      //println("condition - " + ratioDeviation != 99999.0)
      if (ratioDeviation != 99999.0) {
        if (ratioDeviation > threshold) {
          AlertGeneration(realTimeData,histValue, ratioDeviation,path,threshold)
          //LOG.error("Alert is Generated ..Ratio deviation " + ratioDeviation)
        //  println("Alert has Generated ..Ratio deviation " + ratioDeviation)
        } else {
           //println("Alert not Generated ..."+(ratioDeviation > threshold))
          //LOG.error("Alert not Generated ..")
        }
      } else {
       // println("Alert not Generated ...99999")
        //LOG.error("Alert not Generated ..")
      }

    }

  }

  // Parse json data
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

    //LOG.error("LOG generated..")
    // parse input arguments
    val params = ParameterTool.fromArgs(args)

    if (params.getNumberOfParameters < 3) {
      println("Missing parameters!\n"
        + "Usage: Kafka --input-topic <topic> --output-topic <topic> "
        + "--bootstrap.servers <kafka brokers> "
        + "--zookeeper.connect <zk quorum> --group.id <some id> [--prefix <prefix>]")
      return
    }
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //Set up the table SQL execution environment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    //Set parallelism
    //  env.setParallelism(1);
    //Get Historic hashmap Data from Cassandra database

    //Get Historic hashmap Data from Cassandra database

      val HistoricData = getAllDataFromCassandra()
    //env.getConfig.disableSysoutLogging
    //env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    // create a checkpoint every 5 seconds
    //env.enableCheckpointing(5000)
    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // create a Kafka streaming source consumer for Kafka 0.10.x
    print(params.getProperties)
    //val kafkaParams = Map("bootstrap.servers" -> "localhost:9092","zookeeper.connect" -> "localhost:2181","group.id"->"myconsumer")
    val kafkaConsumer = new FlinkKafkaConsumer010(
      //  "test",
      params.getRequired("input-topic"),
      new SimpleStringSchema,
      params.getProperties)
    LOG.error("connected kafka")
    val text = env
      .addSource(kafkaConsumer)


    //val text1 =text.map(m => parseJSON(m))

    val windowSize = params.get("windowsize").toInt //3
    //group by stock symbol and
    // create windows of windowSize records slided every slideSize records
    val msg = text.map(m => parseJSON(m))
      .filter(f => !f._1.contains("Invalid"))
      .keyBy(0).countWindow(windowSize, 1) //.sum(3)
      .apply((key, window, events, out: Collector[(String, String, Double, Double, Double, Double)]) => {
      // val cnt = (events.map(m=>m._3
      out.collect(events.map(m => m._1).last, events.map(m => m._2).last, events.map(m => m._3).last, events.map(m => m._4).last, (events.map(m => m._3).sum / (events.map(m => m._3).seq.size)), (events.map(m => m._4).sum / (events.map(m => m._4).seq.size))) //(cell, window.getEnd, events.map( _._3 ).sum,events.map( _._4 ).sum ))
    })

    //text.print()
    //println(text)
    println("================msg===================")
    //msg.print()
//    LOG.error("processed  kafka data")
    //Calculate price ,volume change and ratio
    val result = msg.map(m => {
      var priceChange: Double = Option(((m._3 - m._5) * 100) / m._5).getOrElse(0.0)
      var volumeChange: Double = Option((((m._4 - m._6) * 100) / m._6)).getOrElse(0.0)
      var ratio: Double = Option((priceChange / volumeChange)).getOrElse(0.0)
      if (ratio.isNaN) ratio = 0.0
      if (priceChange.isNaN) priceChange = 0.0
      if (volumeChange.isNaN) volumeChange = 0.0
      (m._1, m._2, m._3, m._4, m._5, priceChange, m._6, volumeChange, ratio)
    })

    val data = result.map(m =>
      StockMarket((m._1.replace('"', ' ').trim), (m._2.replace('"', ' ').trim), m._3, m._4, m._5, m._6, m._7, m._8, m._9)
    )
    val threshold = params.get("threshold").toDouble
    val path = params.get("path")
    //Compare the Historic and real time data for generate alert
    writeDataIntoIgnite(data)
    val compareData = result.map(m => {

      val key = (m._1, m._2, m._9)

      CompareHistoricAndRealTimeHashmap(key, HistoricData, threshold, path)
       //CompareHistoricAndRealTime(key,threshold,path)
    })


   /* val cassdata = result.map(m => ((m._1.replace('"', ' ').trim),(m._2.replace('"', ' ').trim).split(" ")(1),m._5,m._6,m._7,m._8,m._9,m._9))
    //Write datastream data into Cassandra
     CassandraSink.addSink(cassdata)
         .setQuery("INSERT INTO hedge.stock (symbol ,timestampvalue ,avgprice  ,avgvolume   ,pricechange  ,volumechange,ratio,histratio ) values (?, ? ,?,?,?,?,?,?);")
         .setHost("127.0.0.1")
         .build()*/

    //cassdata.print()
    //    //Register table for executing sql statements..
//    tableEnv.registerDataStream("stock", data)
  //  val rs: Table = tableEnv.scan("stock")

    //val rows: DataStream[Row] = tableEnv.toAppendStream[Row](rs)
    //println("Row printed ..")
    //rows.print()
//      rs.writeToSink(
/*
      new CsvTableSink(
        "C:\\Users\\DL250031\\IdeaProjects\\Flink\\src\\Resources\\output",                             // output path
        fieldDelim = ",",                 // optional: delimit files by '|'
        numFiles = 1,                     // optional: write to a single file
        writeMode = WriteMode.OVERWRITE)
    // Execute the program
*/

    env.execute("RealTimeStreamingDataProcessing")
  }


 }
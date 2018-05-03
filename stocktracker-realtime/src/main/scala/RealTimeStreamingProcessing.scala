//package com.teradata.hedge

import java.{lang, util}
import com.teradata.flinkConnection.Flink_Connector
import com.teradata.mail.SendMail
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.util.Collector

/**
  * --bootstrap.servers localhost:9092 --zookeeper.connect  localhost:2181  --group.id myconsumer --input-topic test --threshold -100 --windowsize 3
*/

object RealTimeStreamingProcessing  extends Utils_scala {
  import org.apache.log4j._
  val LOG = Logger.getLogger(RealTimeStreamingProcessing.getClass)
  LOG.setLevel(Level.ERROR)
  val stringsp = "price"
  val stringsv = "volume"
  val stringst = "timestampValue"
  val stringsn = "symbol"

  def main(args: Array[String]): Unit = {
    LOG.error("===========Started ...===================================")
    val params = ParameterTool.fromArgs(args)// parse input arguments
    if (params.getNumberOfParameters < 3) {
      println("Missing parameters!\n"
        + "Usage: Kafka --input-topic <topic> --output-topic <topic> "
        + "--bootstrap.servers <kafka brokers> "
        + "--zookeeper.connect <zk quorum> --group.id <some id> [--prefix <prefix>]")
      return
    }
    val env = StreamExecutionEnvironment.getExecutionEnvironment  // set up the execution environment
    env.setParallelism(3) //Set parallelism
    //Get Historic hashmap Data from Cassandra database
    val sql = "select * from test.HistStocks;"
    val obj = new Flink_Connector(sql)
    val HistoricData = obj.getHashMap
    import scala.collection.JavaConversions._
    System.out.println("HashMap size "+HistoricData.size())
    println("===================Hashmap=======")
    HistoricData.keys.foreach{ i =>
      print( "Key = " + i )
      println(" Value = " + HistoricData(i) )}
    obj.closeConnection()
    env.getConfig.setGlobalJobParameters(params)  // make parameters available in the web interface
    print(params.getProperties)
    println("1")
    // create a Kafka streaming source consumer for Kafka 0.10.x
    val kafkaConsumer = new FlinkKafkaConsumer010(
      params.getRequired("input-topic"),
      new SimpleStringSchema,
      params.getProperties)
    print("Consumer Created: " + kafkaConsumer)
    val text = env
      .addSource(kafkaConsumer)
    println("2")
    print("text Created: " + text)
    val windowSize = params.get("windowsize").toInt //3
    //group by stock symbol and create windows of windowSize records slided every slideSize records
    val msg = text.map( m => parseJSON(m))
      .filter(f => !f._1.contains("Invalid"))
      .keyBy(0).countWindow(windowSize,1)//.sum(3)
      .apply( (key, window, events, out: Collector[(String, String, Double, Double,Double,Double)]) => {
      // val cnt = (events.map(m=>m._3
      out.collect(events.map(m=>m._1 ).last, events.map(m=>m._2 ).last,events.map(m=>m._3).last,events.map(m=>m._4).last,(events.map(m=>m._3 ).sum/(events.map(m=>m._3 ).seq.size)),(events.map(m=>m._4 ).sum/(events.map(m=>m._4 ).seq.size)))       //(cell, window.getEnd, events.map( _._3 ).sum,events.map( _._4 ).sum ))
    })
    print("msg Created: " + msg)
    //Calculate price ,volume change and ratio
    val result = msg.map( m =>{
      var priceChange : Double= Option(((m._3-m._5)*100)/m._5).getOrElse(0.0)
      var volumeChange  :Double= Option((((m._4-m._6)*100)/m._6)).getOrElse(0.0)
      var ratio :Double = Option((priceChange/volumeChange)).getOrElse(0.0)
      if (ratio.isNaN) ratio=0.0
      if (priceChange.isNaN) priceChange =0.0
      if (volumeChange.isNaN) volumeChange =0.0
      (m._1,m._2,m._3,m._4,m._5,priceChange,m._6,volumeChange,ratio)
    })
    print("result Created: " )
    //println("==================final data--- ")
    result.print()
    val threshold = params.get("threshold").toDouble
    //Compare the Historic and real time data for generate alert
    val compareData =  result.map(m =>{
      val key = (m._1,m._2,m._9)
      CompareHistoricAndRealTime(key,HistoricData,threshold)
    } )
    // Execute the program
    env.execute("RealTimeStreamingDataProcessing")
  }

  // Parse json data
  def parseJSON(m :String) :(String,String,Double,Double) ={
    lazy val jsonParser = new ObjectMapper()
    try{
      val jsonNode = jsonParser.readValue(m,classOf[JsonNode])
      //val stockName =Option(jsonNode.get("stock_symbol")).getOrElse("Invalid").asInstanceOf[String]

      (jsonNode.get("symbol").toString,jsonNode.get("timestampValue").toString,jsonNode.get("price").asDouble(),jsonNode.get("volume").asDouble())

    }catch {case ex:Exception  =>
      println("Unable to parse the json data "+m)
      ("Invalid","Invalid",0.0,0.0)
    }
  }

  //Compare real time data and historic cassandra data
  def CompareHistoricAndRealTime(realTimeData: (String, String, Double),  HistoricData: util.HashMap[String, lang.Double],threshold:Double): Unit ={
    //val key :String  = UtilityClass.concatString(realTimeData._1,realTimeData._2)
    val key : String = ConcatString(realTimeData._1,realTimeData._2)
    val histValue:Double = Option(HistoricData.get(key)).getOrElse(0.0).asInstanceOf[Double]
    //val histValue:Int = Option(historicData.get(key).toInt).getOrElse(0).asInstanceOf[Int]

    println("Key is == "+ key)
    println("Historic Value =>"+ histValue +"Real time data"+realTimeData._3)
    var ratioDeviation :Double = Option(((realTimeData._3-histValue)*100)/histValue).getOrElse(0.0)
    if (ratioDeviation.isNaN) ratioDeviation=0.0

    if(ratioDeviation.isInfinite){
      ratioDeviation=0
    }
    println("Ratio Deviation =>"+ratioDeviation)
    if( ratioDeviation > threshold){
      //AlertGeneration(realTimeData,histValue, ratioDeviation)
      LOG.error("Alert is Generated ..")
      println("Alert is Generated ..")
    } else {
      println("Alert not Generated ...")
      LOG.error("Alert not Generated ..")
    }
  }

  //Send  alert mail as STOCK PRICE ALERTS
  def AlertGeneration(tuple: (String, String, Double), histValue:Double, deviation :Double): Unit ={
    val mail = new SendMail
    val props = mail.loadPropertiesFile
    val s:String = " Alert has generated for stock "+tuple._1+"and timestamp "+
      tuple._2+". The historic ratio for this stock is "+histValue+"and ratio deviation "+deviation
    //mail.send(props, s)
    LOG.error(s)
    println(s)
    ///s.wr  //writeAsText(path).setParallelism(1)
  }

  // Define stockmarket case class
  case class StockMarket(stock_symbol:String,transactionTime:String,price:Double,volume:Long)
}
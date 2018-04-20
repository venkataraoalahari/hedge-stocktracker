import java.{lang, util}
import java.util.Properties

import com.teradata.UtilityClass
import com.teradata.flinkConnection.Flink_Connector
import com.teradata.mail.SendMail
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.java.Tumble
import org.apache.flink.table.api.scala.Slide
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.util.Collector

//import org.apache.flink.table.api.scala.Slide
//import org.apache.flink.table.sinks.CsvTableSink
import  org.apache.flink.streaming.connectors.cassandra.CassandraSink

/**

  * --bootstrap.servers localhost:9092 --zookeeper.connect  localhost:2181  --group.id myconsumer --input-topic test --threshold -100 --windowsize 3
  * }}}
  */


object RealTimeStreamingProcessing  extends Utils_scala {


  def main(args: Array[String]): Unit = {

    import org.apache.log4j._
     val LOG = Logger.getLogger("org")
    LOG.setLevel(Level.ERROR)


    LOG.info("==============================================")
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
    env.setParallelism(3);
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

    val text = env
      .addSource(kafkaConsumer)
    lazy val jsonParser = new ObjectMapper()

    // Parse json data
    def parseJSON(m :String) :(String,String,Double,Double) ={
      try{
        val jsonNode = jsonParser.readValue(m,classOf[JsonNode])
        //val stockName =Option(jsonNode.get("stock_symbol")).getOrElse("Invalid").asInstanceOf[String]

        (jsonNode.get("stock_symbol").toString,jsonNode.get("timestamp").toString,jsonNode.get("price").asDouble(),jsonNode.get("volume").asDouble())

      }catch {case ex:Exception  =>
          println("Unable to parse the json data "+m)
        ("Invalid","Invalid",0.0,0.0)
      }

    }

    val text1 =text.map(m => parseJSON(m))

    val windowSize = params.get("windowsize").toInt //3
      //group by stock symbol and
    // create windows of windowSize records slided every slideSize records
    val msg = text.map( m => parseJSON(m))
      .filter(f => !f._1.contains("Invalid"))
      .keyBy(0).countWindow(windowSize,1)//.sum(3)
      .apply( (key, window, events, out: Collector[(String, String, Double, Double,Double,Double)]) => {
       // val cnt = (events.map(m=>m._3
      out.collect(events.map(m=>m._1 ).last, events.map(m=>m._2 ).last,events.map(m=>m._3).last,events.map(m=>m._4).last,(events.map(m=>m._3 ).sum/(events.map(m=>m._3 ).seq.size)),(events.map(m=>m._4 ).sum/(events.map(m=>m._4 ).seq.size)))       //(cell, window.getEnd, events.map( _._3 ).sum,events.map( _._4 ).sum ))
    })

    text.print()
    //println(text)
    //println("================msg===================")
   // msg.print()

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

    //println("==================final data--- ")
    result.print()

    val threshold = params.get("threshold").toDouble

    //Compare the Historic and real time data for generate alert
   val compareData =  result.map(m =>{

     val key = (m._1,m._2,m._9)

     CompareHistoricAndRealTime(key,HistoricData,threshold)
   } )


    //Write datastream data into Cassandra
/*     CassandraSink.addSink(result)
         .setQuery("INSERT INTO stock (stock_symbol,transaction_time,price,volume) values (?, ? ,?,?);")
         .setHost("localhost")
         .build()*/

    //Register table for executing sql statements..
      tableEnv.registerDataStream("stocks",text1)
          val rs = tableEnv.scan("stocks")
      rs.writeToSink(
      new CsvTableSink(
        "C:\\Users\\DL250031\\IdeaProjects\\Flink\\src\\Resources\\output",                             // output path
        fieldDelim = "|",                 // optional: delimit files by '|'
        numFiles = 1,                     // optional: write to a single file
        writeMode = WriteMode.OVERWRITE))

// Execute the program
    env.execute("RealTimeStreamingDataProcessing")
  }

  //Compare real time data and historic cassandra data
  def CompareHistoricAndRealTime(realTimeData: (String, String, Double),  HistoricData: util.HashMap[String, lang.Double],threshold:Double): Unit ={
    //val key :String  = UtilityClass.concatString(realTimeData._1,realTimeData._2)
     val key : String = ConcatString(realTimeData._1,realTimeData._2)
    val histValue:Double = Option(HistoricData.get(key)).getOrElse(0.0).asInstanceOf[Double]
    //val histValue:Int = Option(historicData.get(key).toInt).getOrElse(0).asInstanceOf[Int]

//    println("Key is == "+ key)
  //  println("Historic Value =>"+ histValue +"Real time data"+realTimeData._3)
    var ratioDeviation :Double = Option(((realTimeData._3-histValue)*100)/histValue).getOrElse(0.0)
    if (ratioDeviation.isNaN) ratioDeviation=0.0

    if(ratioDeviation.isInfinite){
      ratioDeviation=0
    }
    //println("Ratio Deviation =>"+ratioDeviation)
    if( ratioDeviation > threshold){
     // AlertGeneration(realTimeData,histValue, ratioDeviation)
     // println("Alert is Generated ..")
    } else {
      //println("Alert not Generated ...")
    }
  }


       //Send  alert mail as STOCK PRICE ALERTS
  def AlertGeneration(tuple: (String, String, Double), histValue:Double, deviation :Double): Unit ={
    val mail = new SendMail
    val props = mail.loadPropertiesFile
    val s:String = " Alert has generated for stock "+tuple._1+"and timestamp "+
     tuple._2+". The historic ratio for this stock is "+histValue+"and ratio deviation "+deviation
    mail.send(props, s)
    println(s)
    ///s.wr  //writeAsText(path).setParallelism(1)
  }
 // Define case stockmarket class
  case class StockMarket(stock_symbol:String,transactionTime:String,price:Double,volume:Long)
}
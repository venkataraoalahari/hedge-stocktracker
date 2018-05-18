package com.hedge.spark

import org.slf4j.LoggerFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import net.liftweb.json._
import java.util.Calendar

object GetKafkaData {
  val logger = LoggerFactory.getLogger(getClass)

  case class StockRecord(symbol: String, timestampValue: String, price: Double, volume: Int)
  
  implicit val formats = DefaultFormats
  def parseJson(line: String): StockRecord = {

    val jValue = parse(line)
    val sr = jValue.extract[StockRecord]

    (sr)
  }
  
  def main(args: Array[String]): Unit = {
    val topicName = args

    val warehouseLocation = "hedge-spark-warehouse";
    val spark = SparkSession.builder().appName("Get Kafka Data").master("local[2]")
				.config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate();
    
    val sc = spark.sparkContext
  //  val sparkConf = new SparkConf().setAppName("GetKafkaData")
  //   val ssc = new StreamingContext(sparkConf, Seconds(1))
    val ssc = new StreamingContext(sc, Seconds(60))
    
    val kafkaParams = Map(
      "bootstrap.servers" -> "sandbox.kylo.io:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "mygroup1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "true")

    val messages = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicName, kafkaParams))
  
    val lines = messages.map(kafkaRecord => kafkaRecord.value())
    
    import spark.implicits._
    lines.foreachRDD{(rdd,time) => 
        if(rdd.count() > 0){
          val dfStock = rdd.map(line => parseJson(line)).toDF()
   //       dfStock.show()
          dfStock.createOrReplaceTempView("stock")
          spark.sql("set hive.exec.dynamic.partition=true")
          spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
          
          val startTime = Calendar.getInstance
          logger.info("Hive Insertion Starts at time =========> " +startTime.get(Calendar.HOUR)+":"+startTime.get(Calendar.MINUTE)+":"+startTime.get(Calendar.SECOND))
          spark.sql("insert into table hedge.stock_tracker partition(symbol) select timestampValue,price,volume,symbol from stock")
       //   spark.sql("insert into table hedge.stock_tracker select symbol,timestampValue,price,volume from stock")
       
          val endTime = Calendar.getInstance
          logger.info("Hive Insertion Ends at time   =========> " +endTime.get(Calendar.HOUR)+":"+endTime.get(Calendar.MINUTE)+":"+endTime.get(Calendar.SECOND))
        }
    }
    
    ssc.start()
    ssc.awaitTermination()
  }
}
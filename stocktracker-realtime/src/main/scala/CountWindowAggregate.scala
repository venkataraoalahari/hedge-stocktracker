import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

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



object CountWindowAggregate {

  def main(args: Array[String]) : Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("192.168.152.128", 9000, '}', 1)

    lazy val jsonParser = new ObjectMapper()


    val ds: DataStream[(String, String, Double, Double)] = text.map(m => {
      val jsonNode = jsonParser.readValue(m, classOf[JsonNode])
      (jsonNode.get("stock_symbol").toString, jsonNode.get("timestamp").toString, jsonNode.get("price").asDouble(), jsonNode.get("volume").asDouble())
    })
//ds.print()
    val result = ds.keyBy(0).countWindow(3,1).sum(3).print()

    //val rs = ds.keyBy(0).countWindow(3, 1)


     /* .apply( (key, window, events, out: Collector[(String, String, Double, Double)]) => {
      out.collect(events.map(m=>m._1 ).last, events.map(m=>m._2 ).last,events.map(m=>m._1 ).sum.toDouble,events.map(m=>m._1 ).sum.toDouble)        //(cell, window.getEnd, events.map( _._3 ).sum,events.map( _._4 ).sum ))
    })*/

    env.execute("Socket Window WordCount")
  }
  case class StockMarket(stock_symbol:String,transactionTime:String,price:Double,volume:Long)
}


/*class MyWindowFunction extends


class MyWindowFunction extends WindowFunction[WordWithCount, String, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: lang.Iterable[WordWithCount], out: Collector[String]): Unit = {
    val discount = input.map(t => t.word).toSet.size
    out.collect(s"Distinct elements: $discount")
  }
  def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[String]): Unit = {
    apply(key, window, input.asJava, out)
  }*/

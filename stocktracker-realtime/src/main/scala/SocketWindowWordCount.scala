
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.util.Collector
object SocketWindowWordCount {

  case  class Stocks(name:String,DateTime:String,price:Double,volume:Long)

  def main(args: Array[String]): Unit = {
    println("started..")
 // get the execution environment
    val env =StreamExecutionEnvironment.getExecutionEnvironment
    // get input data by connecting to the socket
    val text = env.socketTextStream("192.168.152.130",9000)

    lazy val jsonParser = new ObjectMapper()

     // parse the data, group it, window it, and aggregate the counts
    val windowSize= 3
     val slideWindowSize =1

    val msg = text.map( m => {
      val jsonNode = jsonParser.readValue(m,classOf[JsonNode])
      (jsonNode.get("stock_symbol").toString,jsonNode.get("timestamp").toString,jsonNode.get("price").asDouble(),jsonNode.get("volume").asDouble())
    }).keyBy(0).countWindow(3,1)//.sum(3)
     .apply( (key, window, events, out: Collector[(String, String, Double, Double)]) => {
          out.collect(events.map(m=>m._1 ).last, events.map(m=>m._2 ).last,events.map(m=>m._3 ).sum.toDouble,events.map(m=>m._4 ).sum.toDouble)        //(cell, window.getEnd, events.map( _._3 ).sum,events.map( _._4 ).sum ))
        }).print()


//    val result: DataStream[(String, Long)] = text
//      // split up the lines in pairs (2-tuples) containing: (word,1)
//      .flatMap(_.toLowerCase.split("\\s"))
//      .filter(_.nonEmpty)
//      .map((_, 1L))
//      // group by the tuple field "0" and sum up tuple field "1"
//      .keyBy(0)
//        //.timeWindow(Time.seconds(5))
//          .countWindow(3,1)
//      .sum(1)



   /* CassandraSink.addSink(result)
      .setQuery("INSERT INTO example.wordcount(word, count) values (?, ?);")
      .setHost("127.0.0.1")
      .build()*/




   // result.print().setParallelism(1)




    /*.map(m =>(m._1.toString,m._2.toString,m._3.asDouble(),m._4.asLong()) )
          .keyBy(0)
      .countWindow(windowSize,slideWindowSize)
*/

 env.execute("Socket Window WordCount")
  }


}

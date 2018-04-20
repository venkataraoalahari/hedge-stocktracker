

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Implements a streaming windowed version of the "WordCount" program.
  *
  * This program connects to a server socket and reads strings from the socket.
  * The easiest way to try this out is to open a text sever (at port 12345)
  * using the ''netcat'' tool via
  * {{{
  * nc -l 12345
  * }}}
  * and run this example with the hostname and the port as arguments..
  */

object WordCountToCassandra {
  /** Main program method */
  def main(args: Array[String]) : Unit = {

    // the host and the port to connect to
    var hostname: String = "192.168.152.128"
    var port: Int = 12345



    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
   // val text: DataStream[String] = env.socketTextStream(hostname, port, '\n')

    val text = env.socketTextStream("192.168.152.128",9000,'}', 1)
    // parse the data, group it, window it, and aggregate the counts
    val result: DataStream[(String, Long)] = text
      // split up the lines in pairs (2-tuples) containing: (word,1)
      .flatMap(_.toLowerCase.split("\\s"))
      .filter(_.nonEmpty)
      .map((_, 1L))
      // group by the tuple field "0" and sum up tuple field "1"
      .keyBy(0)
        .countWindow(4)
        //.timeWindow(Time.seconds(5))

      .sum(1)

    CassandraSink.addSink(result)
      .setQuery("INSERT INTO test.wordCount (word, count) values (?, ?);")
      .setHost("192.168.152.128")
      .build()
    result.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

}

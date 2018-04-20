import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object WindowWordCount {

  def main(args: Array[String]): Unit = {
    println("Started..")

    val param = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text =  env.readTextFile(param.get("input"))
      //Make parameter available in web UI
      env.getConfig.setGlobalJobParameters(param)

      val windowSize = param.get("window")
      val slideSize = param.get("slide")

    val counts: DataStream[(String, Int)] = text
      // split up the lines in pairs (2-tuple) containing: (word,1)
      .flatMap(_.toLowerCase.split(","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      // create windows of windowSize records slided every slideSize records
      //.countWindow(windowSize, slideSize)
         // .countWindow(windowSize.toLong,slideSize.toLong)
      .countWindow(10)
      // group by the tuple field "0" and sum up tuple field "1"
      .sum(1)

    counts.print()
    //counts.writeAsCsv("C:\\Users\\DL250031\\IdeaProjects\\Flink\\src\\Resource")

    env.execute("WindowWordCount")


  }

}

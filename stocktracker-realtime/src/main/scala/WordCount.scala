
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._



object WordCount {
  def main(args: Array[String]): Unit = {

    println("DEVA")
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // get input data
    val text =
    // read the text file from given input path
      if (params.has("input")) {
        env.readTextFile(params.get("input"))
      } else {
        println("Executing WordCount example with default inputs data set.")
        println("Use --input to specify file input.")
        // get default test text data
        env.readTextFile(params.get("input"))
      }

   // import org.apache.flink.api.common.typeinfo._
   //implicit val typeInfo = TypeInformation.of(classOf[(Int, String)])
    val counts: DataStream[(String, Int)] = text
      // split up the lines in pairs (2-tuples) containing: (word,1)
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      // group by the tuple field "0" and sum up tuple field "1"
      .keyBy(0)
      .sum(1)
     counts.print()

    counts.setParallelism(1).writeAsCsv("C:\\Users\\DL250031\\IdeaProjects\\Flink\\src\\Resource\\output")
    // execute program
    env.execute("Streaming WordCount")
  }


}

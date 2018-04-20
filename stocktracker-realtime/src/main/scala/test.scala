
object test {

  def main(args: Array[String]): Unit = {

    val realTimeData: (String, String, Double) = ("deva","dev",0)



    val result = Array(realTimeData._1.toString,realTimeData._2.toString).mkString("_")

    println(result)
  }

}

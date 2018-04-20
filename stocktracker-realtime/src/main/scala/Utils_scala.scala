
trait Utils_scala {

  def ConcatString(s1:String,s2:String):String={

    val s3 = s1.replace('"', ' ').trim + "_" + s2.replace('"', ' ').trim

    return s3
  }

}

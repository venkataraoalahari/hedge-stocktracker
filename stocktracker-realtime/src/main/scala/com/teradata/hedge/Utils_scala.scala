package com.teradata.hedge


trait Utils_scala {

  def ConcatString(s1:String,s2:String):String={

    val s3 = s1.replace('"', ' ').trim + "_" + ((s2.replace('"', ' ').trim).split(" ")(1).substring(0,8))

    return s3
  }

}

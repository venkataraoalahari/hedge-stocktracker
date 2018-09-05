package com.hedge.spark

import org.apache.spark.sql.SparkSession
//import com.datastax.bdp.spark.writer.BulkTableWriter._
import com.datastax.spark.connector._
import org.slf4j.LoggerFactory
import org.apache.ignite.spark.IgniteDataFrameSettings._

object SparkTransformationIgnite {
  val logger = LoggerFactory.getLogger(getClass)
  def main(args: Array[String]): Unit = {
    val windowLength = args(0).toInt
    val configFile = args(1)
    
    val actualWindow = windowLength - 1
    val warehouseLocation = "hdfs://localhost:9000/hedge-spark-warehouse";
    val spark = SparkSession.builder().appName("Hedge-Spark Transformation").master("local[2]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.hadoop.fs.defaultFS","hdfs://localhost:9000")
      .config("fs.defaultFS","hdfs://localhost:9000")
      .enableHiveSupport().getOrCreate();

    
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    //val ratioQuery = "INSERT INTO TABLE hedge.stock_transformation_temp partition(symbol) " +
    val ratioQuery = "INSERT INTO TABLE hedge.stock_transformation_temp " +
      "SELECT t1.symbol,t1.timestampValue,t1.avgprice,t1.pricechange,t1.avgvolume,t1.volumechange,NVL(t1.pricechange/t1.volumechange,0) as ratio FROM " +
      "(SELECT t.symbol,t.timestampValue,t.price,t.volume,t.avgprice,t.avgvolume,((t.price-t.avgprice)*100/t.avgprice) as pricechange, ((t.volume-t.avgvolume)*100/t.avgvolume) as volumechange FROM " +
      "(SELECT symbol,timestampValue,price,volume,AVG(price) OVER w AS avgprice,AVG(volume) OVER w AS avgvolume FROM hedge.deltadata " +
      "WINDOW w AS (PARTITION BY symbol ORDER BY timestampValue ROWS BETWEEN " + actualWindow + " PRECEDING AND CURRENT ROW)) t) t1"

    spark.sql(ratioQuery)
    logger.info("Ratio Query Completed Successfully!!")

    val histRatioQuery = "select t2.symbol,t1.timeHHmmSS as timestampvalue,round(t2.avgprice,5) as avgprice,round(t2.pricechange,5) as pricechange,round(t2.avgvol,5) as avgvolume,round(t2.volumechange,5) as volumechange,round(t2.ratio,5) as ratio,round(t1.historicalratio,5) as histratio from " +
      "(select symbol,substring(timestampValue,12,8) as timeHHmmSS,avg(ratio) as historicalratio " +
      "from hedge.stock_transformation_temp group by symbol,substring(timestampValue,12,8) )t1 join " +
      "(select symbol,timestampValue,avgprice,pricechange,avgvol,volumechange,ratio from hedge.stock_transformation_temp " +
      "where timestampValue in( select max(timestampValue) from hedge.stock_transformation_temp group by symbol,substring(timestampValue,12,8))) t2 " +
      "on t1.symbol=t2.symbol and t1.timeHHmmSS=substring(t2.timestampValue,12,8)"

    val histRatioDf = spark.sql(histRatioQuery)
    logger.info("Historical Ratio Query Completed Successfully!!")

    //   histRatioDf.write.mode("append").format("org.apache.spark.sql.cassandra").options(Map("table" -> "stock", "keyspace" -> "hedge")).save()

    histRatioDf.write.mode("overwrite")
      .format(FORMAT_IGNITE)
      .option(OPTION_CONFIG_FILE, configFile)
      .option(OPTION_TABLE, "stock_transformation")
      .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "symbol,timestampvalue")
      .option(OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
      .save()
    logger.info("Ignite Load Completed Successfully!!")
    
    System.exit(0)
  }
}
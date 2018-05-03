package com.hedge.spark

import org.apache.spark.sql.SparkSession
//import com.datastax.bdp.spark.writer.BulkTableWriter._
import com.datastax.spark.connector._
import org.slf4j.LoggerFactory

object SparkTransformation {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
    val warehouseLocation = "hedge-spark-warehouse";
    val spark = SparkSession.builder().appName("Hedge-Spark Transformation").master("local[2]")
      .config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate();

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val ratioQuery = "insert into table hedge.stock_transformation_temp partition(symbol) select stockcal.timestampValue,stockcal.avgprice,stockcal.pricechange,stockcal.avgvol,stockcal.volumechange,NVL(stockcal.pricechange/stockcal.volumechange,0) as ratio,stockcal.symbol from " +
      "(select test.symbol,test.avgprice,NVL((temp.price-test.avgprice)*100/test.avgprice,0) as pricechange, test.avgvol, " +
      "NVL((temp.volume-test.avgvol)*100/test.avgvol,0) as volumechange, temp.timestampValue from " +
      "(select symbol,substring(timestampValue,12,8) timeHHmmSS,avg(price) avgprice,avg(volume) avgvol from hedge.stock_tracker group by symbol,substring(timestampValue,12,8))test join " +
      "(select symbol,price,volume,timestampValue from hedge.stock_tracker where timestampValue in " +
      "(select max(timestampValue) from hedge.stock_tracker group by symbol,substring(timestampValue,12,8)))temp " +
      "on test.symbol=temp.symbol and test.timeHHmmSS=substring(temp.timestampValue,12,8)) stockcal"

    spark.sql(ratioQuery)
    logger.info("Ratio Query Completed Successfully!!")
    
    val histRatioQuery = "select t2.symbol,t1.timeHHmmSS as timestampvalue,t2.avgprice,t2.pricechange,t2.avgvol as avgvolume,t2.volumechange,t2.ratio,t1.historicalratio as histratio from " +
      "(select symbol,substring(timestampValue,12,8) as timeHHmmSS,avg(ratio) as historicalratio " +
      "from hedge.stock_transformation_temp group by symbol,substring(timestampValue,12,8) )t1 join " +
      "(select symbol,timestampValue,avgprice,pricechange,avgvol,volumechange,ratio from hedge.stock_transformation_temp " +
      "where timestampValue in( select max(timestampValue) from hedge.stock_transformation_temp group by symbol,substring(timestampValue,12,8))) t2 " +
      "on t1.symbol=t2.symbol and t1.timeHHmmSS=substring(t2.timestampValue,12,8)"

    val histRatioDf = spark.sql(histRatioQuery)
    logger.info("Historical Ratio Query Completed Successfully!!")
    
    histRatioDf.write.mode("append").format("org.apache.spark.sql.cassandra").options(Map("table" -> "stock", "keyspace" -> "hedge")).save()
  }
}
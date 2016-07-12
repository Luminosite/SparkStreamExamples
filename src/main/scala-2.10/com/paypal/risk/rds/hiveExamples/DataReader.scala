package com.paypal.risk.rds.hiveExamples

import com.paypal.risk.rds.hiveExamples.SnowballRadd._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame}

/**
  * Created by kufu on 07/07/2016.
  */
object DataReader extends sqlContextHelper {
  val appName = "SnowballRadd"
  val baseSnowballPath = "/home/kufu/risk/dst/BSL"
  val basePath = ""
  val partitionNum = 3000

  def getSnowballPath(date: String):String = {
    baseSnowballPath + "/" + date + "/00"
  }

  def getDataPath(date: String):String = {
    basePath + "/" + date
  }

  def radd2table(curData: DataFrame):Unit = {

  }

  def table2radd(snowball: DataFrame):Unit = {

  }

  def sp(str:String):Row = {
    val strs = str.split(",")
    Row(strs(0), strs(1))
  }

  def main(args: Array[String]) {
    val date = "2015-04-07"
    val snowballPath = getSnowballPath(date)
    val sc = getSparkContext(appName)
    val sqlContext = new HiveContext(sc)

    val schema = StructType(StructField("id", StringType)::StructField("value", StringType)::Nil)
    val data = sc.textFile(snowballPath).map[Row](sp)
    val snowball = sqlContext.createDataFrame(data, schema)
    snowball.show(2400)

    sc.stop()
  }
}
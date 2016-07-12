package com.paypal.risk.rds.hiveExamples

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kufu on 7/6/2016.
  */
object SnowballRadd extends sqlContextHelper {
  val appName = "SnowballRadd"
  val baseSnowballPath = "/home/kufu/risk/dst/BSL_Snowball"
  val basePath = "/home/kufu/risk/dst/BSL"
  val partitionNum = 3000

  def getSnowballPath(date: String):String = {
    baseSnowballPath + "/" + date + "/00"
  }

  def getDataPath(date: String):String = {
    basePath + "/" + date + "/00"
  }

  def sp(str:String):Row = {
    if(!str.contains(',')){
      throw new Exception("Exception in input rdd formation, ',' doesn't exist.")
    }
    val strs = str.split(",")
    Row(strs(0), strs(1))
  }

  def main(args: Array[String]) {
    val pit = "2015-06-09"
    val today = "2015-06-16"

    if(pit.equals(today)){

      val curData = getDataPath(today)
      val output = getSnowballPath(today)
      val sc = getSparkContext(appName)

      val rdd = readRDD(sc, curData)
      saveRDD(rdd, output)

      sc.stop()
    }else {

      val curPath = getDataPath(today)
      val snowPath = getSnowballPath(pit)

      val context = getContext(appName)

      val curTable = getDataFrame(context._1, context._2, curPath)
      val snowTable = getDataFrame(context._1, context._2, snowPath)

      val curTableName = "cur_radd_table"
      val snowTableName = "snowball_table"
      val attr_id = "id"
      val attr_value = "value"
      curTable.registerTempTable(curTableName)
      snowTable.registerTempTable(snowTableName)

      val ons = s"$snowTableName.$attr_id = $curTableName.$attr_id"
      val newSnowball = context._2.sql{
        s"""
           |SELECT coalesce($snowTableName.$attr_id, $curTableName.$attr_id) as id,
           |coalesce($snowTableName.$attr_value, $curTableName.$attr_value) as value
           |FROM $snowTableName FULL OUTER JOIN $curTableName ON $ons ORDER BY id
           """.stripMargin
      }

      newSnowball.show(2400)

      val rdd = getRDD(newSnowball)
      val output = getSnowballPath(today)
      saveRDD(rdd, output)

      context._1.stop()
    }
  }

  def saveRDD(rdd:RDD[String], path:String):Unit = {
    rdd.saveAsTextFile(path, classOf[GzipCodec])
  }

  def readRDD(sc:SparkContext, path:String):RDD[String] = {
    sc.textFile(path)
  }

  def getRDD(df:DataFrame):RDD[String] = {
    df.rdd.map[String](row=>row.getString(0)+","+row.getString(1))
  }

  def getDataFrame(sc:SparkContext, sqlContext: HiveContext, path:String):DataFrame = {

    val schema = StructType(StructField("id", StringType)::StructField("value", StringType)::Nil)
    val data = readRDD(sc, path).map[Row](sp)
    sqlContext.createDataFrame(data, schema)

  }
}

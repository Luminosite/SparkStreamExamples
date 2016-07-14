package com.paypal.risk.rds.hiveExamples

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

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

  val sc = getSparkContext("ss")

  def compareD2(oldPath:String, newPath:String, date:String):Unit = {
    val oldR = sc.textFile(s"$oldPath/$date/00")
    val oldData = oldR.map[String](str=>str.substring(0, str.indexOf(',')))
    val newR = sc.textFile(s"$newPath/$date/00")
    val newData = newR.map[String](str=>str.substring(0, str.indexOf(',')))
    val o_n = oldData.subtract(newData).collect().length
    val n_0 = newData.subtract(oldData).collect().length
    println("old-new:"+o_n+"\nnew-old:"+n_0)
    println()
  }

  def compareO(curPath:String, snowPath:String, oldDate:String, curDate:String):Unit = {
    def getId(str:String):String = str.substring(0, str.indexOf(','))
    val curData = sc.textFile(s"$curPath/$curDate/00").map[String](getId)
    val snowData = sc.textFile(s"$snowPath/$curDate/00").map[String](getId)
    val oldBall = sc.textFile(s"$snowPath/$oldDate/00").map[String](getId)
    val d_s = curData.subtract(snowData).collect().length
    val s_d = snowData.subtract(curData)
    val s_dNotInOldBall = s_d.subtract(oldBall).collect().length
    val o_s = oldBall.subtract(snowData).collect().length
    val s_o = snowData.subtract(oldBall)
    val s_oNotInCurData = s_o.subtract(curData).collect().length

    println(d_s+":data-snowball should be 0\n"+
      s_dNotInOldBall+":snowball-data should from old, count of not should be 0\n"+
      o_s+":old snowball has no more data than new, it should be 0\n"+
      s_oNotInCurData+":snowball - old one should from curData, count of not should be 0"
    )
  }

  def compareO3(curPath:String, snowPath:String, oldDate:String, curDate:String):Unit = {
    def getId(str:String):String = str.substring(0, str.indexOf(','))
    val cur = sc.textFile(s"$curPath/$curDate/00")
    val snow = sc.textFile(s"$snowPath/$curDate/00")
    val old = sc.textFile(s"$snowPath/$oldDate/00")

    val snow_old = snow.subtract(old)
    val moreRaddNum = snow_old.collect().length
    val snow_oldNotInCurData = snow_old.subtract(cur).collect().length

    println(
      moreRaddNum+":there may be some updated RADDs and added RADDs, more than 0\n"+
      snow_oldNotInCurData+":all different or more RADDs should be in current RADD, it should be 0\n"
    )
  }

  def compareO2(curPath:String, snowPath:String, oldDate:String, curDate:String):Unit = {
    def getId(str:String):String = str.substring(0, str.indexOf(','))
    val cur = sc.textFile(s"$curPath/$curDate/00")
    val curData = cur.map[String](getId)
    val snow = sc.textFile(s"$snowPath/$curDate/00")
    val snowData = snow.map[String](getId)
    val old = sc.textFile(s"$snowPath/$oldDate/00")
    val oldBall = old.map[String](getId)
    val d_s = curData.subtract(snowData).collect().length
    val s_d = snowData.subtract(curData)
    val s_dNotInOldBall = s_d.subtract(oldBall).collect().length
    val o_s = oldBall.subtract(snowData).collect().length
    val s_o = snowData.subtract(oldBall)
    val s_oNotInCurData = s_o.subtract(curData).collect().length

    val moreIdNum = s_o.collect().length
    val snow_old = snow.subtract(old)
    val moreRaddNum = snow_old.collect().length
    val diffRaddNum = moreRaddNum-moreIdNum
    val snow_oldNotInCurData = snow_old.subtract(cur).collect().length

    println(d_s+":data-snowball should be 0\n"+
      s_dNotInOldBall+":snowball-data should from old, count of not should be 0\n"+
      o_s+":old snowball has no more data than new, it should be 0\n"+
      s_oNotInCurData+":snowball - old one should from curData, count of not should be 0"+
      diffRaddNum+":there may be some updated RADDs, more than 0"+
      snow_oldNotInCurData+":all different or more RADDs should be in current RADD, it should be 0"
    )
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

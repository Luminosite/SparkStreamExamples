package com.paypal.risk.rds.hiveExamples

import java.text.SimpleDateFormat
import org.apache.hadoop.io.compress.GzipCodec
import org.joda.time.DateTime
import scala.collection.mutable.ListBuffer

/**
  * Created by kufu on 07/07/2016.
  */
object RaddGenerator extends sqlContextHelper {
  val appName="RaddGenerator"

  def genData(start: Int, base: Int): List[String] = {
    val list = new ListBuffer[String]
//  val appName = "SnowballRadd"
//  val baseSnowballPath = "/home/kufu/risk/dst/BSL"
//  val basePath = ""
//  val partitionNum = 3000
//
//  val sqlContext = getHiveContext(appName)


    (start+1 to base).foreach(index => {
      list+=s"cust_id_$index,2|2|3|seg|0|0|0|0|0|0|0|2013-1-1"
    })

    list.toList
  }

  def main(args: Array[String]) {
    val date = new DateTime(2015, 5, 12, 5, 5)
    var start = 0
    var base = 150
    val fList = new ListBuffer[String]

    val sc = getSparkContext(appName)

    (1 to 5).foreach(index => {

      if(index>1){
        val size = fList.size
        val c = 2
        var i = 1
        while(i<size){
          fList.remove(size-i)
          i*=c
        }
      }

      fList.appendAll(genData(start, base))
//      fList.toList.foreach(println(_))

      val list = fList.toList

      val newDate = date.plusDays(index*7)
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
      val newDateStr = dateFormat.format(newDate.toDate)

      sc.makeRDD[String](list).saveAsTextFile(s"/home/kufu/risk/dst/BSL/$newDateStr/00",
        classOf[GzipCodec])

      start=base
      base*=2

      println(s"done for $index")
    })

    sc.stop()
  }
}

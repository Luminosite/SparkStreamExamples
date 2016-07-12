package com.paypal.risk.rds.dataAccessExample

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by kufu on 29/06/2016.
  */
object SqlContextDemo {

  def showJson(sc: SparkContext) = {
    val sqlContext = new SQLContext(sc)
    val testD = sqlContext.read.json("/home/kufu/tmp/test.json")
    testD.show()
  }

  def showSelect(sc: SparkContext) = {
    val list = List("a a a b", "c d e d")

    val wordCount = sc.makeRDD(list).flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
    val sqlContext = new SQLContext(sc)
    val testD = sqlContext.sql("select * from table")
    testD.show()
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[2]").setAppName("SomeSparkDemo")
      .setSparkHome("/home/kufu/spark/spark-1.5.2-bin-hadoop2.6")
      .setExecutorEnv("spark.executor.extraClassPath","target/scala-2.11/sparkstreamexamples_2.11-1.0.jar")

    val sc = new SparkContext(conf)

//    showJson(sc)
    showSelect(sc)

    sc.stop()
  }
}

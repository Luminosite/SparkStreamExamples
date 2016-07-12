package com.paypal.risk.rds.hiveExamples

import org.apache.spark.sql.hive.HiveContext
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

/**
  * Created by kufu on 07/07/2016.
  */
trait sqlContextHelper extends LazyLogging{

  val defaultPartitionNums = 3000

  def getHiveContext(
                      appName: String,
                      partition: Option[Int]=Some(defaultPartitionNums),
                      configs: Map[String, String]=Map()
                    ): SQLContext = {

    val conf = new SparkConf()
    configs.foreach(config => conf.set(config._1, config._2))
    val sparkConf = conf.setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use edw")
    if(partition.isDefined){
      logger.info(s"Setting partitions to ${partition.get}")
      sqlContext.setConf("spark.sql.shuffle.partitions", partition.get.toString)
    }
    sqlContext
  }

  def getSparkContext(
                       appName: String,
                       configs: Map[String, String]=Map(
                         "spark.master"->"local[2]",
                         "spark.home"->"/home/kufu/spark/spark-1.5.2-bin-hadoop2.6"
                       )
                     ): SparkContext = {
    val conf = new SparkConf()
    conf.setAppName(appName)
    configs.foreach(config => conf.set(config._1, config._2))

    new SparkContext(conf)
  }

  def getContext(
                   appName: String,
                   configs: Map[String, String]=Map(
                     "spark.master"->"local[2]",
                     "spark.home"->"/home/kufu/spark/spark-1.5.2-bin-hadoop2.6"
                   )
                 ): (SparkContext, HiveContext) = {
    val conf = new SparkConf()
    conf.setAppName(appName)
    configs.foreach(config => conf.set(config._1, config._2))
    val sc = new SparkContext(conf)
    (sc, new HiveContext(sc))
  }
}

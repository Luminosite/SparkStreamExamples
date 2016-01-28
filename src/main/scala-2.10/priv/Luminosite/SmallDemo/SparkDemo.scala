package priv.Luminosite.SmallDemo

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by kufu on 27/1/16.
  */
object SparkDemo {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[2]").setAppName("SomeSparkDemo")
      .setSparkHome("/home/kufu/spark/spark-1.5.2-bin-hadoop2.6")
      .setExecutorEnv("spark.executor.extraClassPath","target/scala-2.11/sparkstreamexamples_2.11-1.0.jar")

    val sc = new SparkContext(conf)
    val list = List("a a a b", "c d e d")

    val wordCount = sc.makeRDD(list).flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
    wordCount.foreach(tuple=>{
      println(tuple._1+":"+tuple._2)
    })
    sc.stop()
  }
}

package priv.Luminosite.SmallDemo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by kufu on 28/1/16.
  */
object SparkStreamDemo {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[2]").setAppName("KafkaWordCount")
      .setSparkHome("/home/kufu/spark/spark-1.5.2-bin-hadoop2.6")
      .setExecutorEnv("spark.executor.extraClassPath","target/scala-2.11/sparkstreamexamples_2.11-1.0.jar")
//    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(conf, Seconds(2))

    val topicMap = Map("myTopic"->3)
    val wordCounts = KafkaUtils.createStream(ssc, "localhost:2181", "testKafkaGroupId", topicMap)
      .flatMap(_._2.split(" ")).map((_, 1)).reduceByKey(_+_)

    wordCounts.foreachRDD(rdd=>{
      println("--------- a rdd: ----------")
      rdd.foreach(tuple=>{
        println(tuple._1+":"+tuple._2)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
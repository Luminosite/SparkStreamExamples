package com.paypal.risk.rds.SmallDemo

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by kufu on 28/1/16.
  */
object SparkStreamDemo {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[8]").setAppName("KafkaWordCount")
      .setSparkHome("/home/kufu/spark/spark-1.5.2-bin-hadoop2.6")
      .setExecutorEnv("spark.executor.extraClassPath","target/scala-2.11/sparkstreamexamples_2.11-1.0.jar")
      .set("spark.cores.max", "8")

    val ssc = new StreamingContext(conf, Seconds(2))

    val topicMap = Map("myTopic"->1)

//    val seq = (1 to 2).map(_=>KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder]
//      (ssc, Map("bootstrap.servers"->"localhost:9092"), Set("myTopic")))

    val kafkPara = Map("broker.id"->"0",
                        "zookeeper.connect"->"localhost:2181")
    val seq = (1 to 2).map(i=>KafkaUtils.createStream[String, String, StringDecoder, StringDecoder]
      (ssc, kafkPara, Map("myTopic"->3), StorageLevel.MEMORY_ONLY_SER))

    val wordCounts = ssc.union(seq)

    wordCounts.foreachRDD(rdd=>{
      println("--------- a rdd: ----------")
      Thread.sleep(5000)

      rdd.foreach(tuple=>{
        println(tuple)
      })

      println("---------------------------")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
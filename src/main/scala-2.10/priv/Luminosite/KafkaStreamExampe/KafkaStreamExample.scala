package priv.Luminosite.KafkaStreamExampe

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import priv.Luminosite.HBase.util.{HTableData, HBaseConnection}
import priv.Luminosite.KafkaStreamExampe.OutputComponent.{SimpleKafkaProducer, ExampleDataTransfer, TableIncrementFormat}

/**
  * Created by kufu on 28/1/16.
  */
class KafkaStreamExample {

  def run(approachType:Int, consumeTopic:String, zkOrBrokers:String, publishTopic:String, publishers:List[String]): Unit ={

    val conf = new SparkConf()
    conf.setMaster("local[2]").setAppName("KafkaStreamExample")
      .setSparkHome("/home/kufu/spark/spark-1.5.2-bin-hadoop2.6")
      .setExecutorEnv("spark.executor.extraClassPath","target/scala-2.11/sparkstreamexamples_2.11-1.0.jar")

    val ssc = new StreamingContext(conf, Seconds(2))
    val topicMap = Map(consumeTopic->3)

    val dataRDD:InputDStream[(String, String)] = approachType match {
      case KafkaStreamExample.ReceiverBasedApproach =>
        KafkaUtils.createStream(ssc, zkOrBrokers, "testKafkaGroupId", topicMap)
      case KafkaStreamExample.DirectApproach =>
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, Map[String, String]("metadata.broker.list" -> zkOrBrokers),
          Set[String](consumeTopic))
    }

    val wordCounts = dataRDD.flatMap(_._2.split(" ")).map((_, 1)).reduceByKey(_+_)

    val hbaseConf = HBaseConfiguration.create()
    conf.set(TableIncrementFormat.OUTPUT_TABLE, KafkaStreamExample.rawDataTable)
    val jobConf:JobConf = new JobConf(hbaseConf, this.getClass)
    jobConf.setOutputFormat(classOf[TableIncrementFormat])
    jobConf.set(TableIncrementFormat.OUTPUT_TABLE, KafkaStreamExample.rawDataTable)

    wordCounts.foreachRDD(genProcessing(publishTopic, publishers, jobConf))

    ssc.start()
    ssc.awaitTermination()
  }

  def genProcessing(publishTopic:String, publishers:List[String], jobConf:JobConf):(RDD[(String, Int)])=>Unit = {
    def eachRDDProcessing(rdd:RDD[(String, Int)]):Unit = {
      println("--------- a rdd: ----------")
      val newRdd = rdd.repartition(2)
      newRdd.foreach(tuple => {
        Thread.sleep(1500)
        println(tuple._1 + ":" + tuple._2)
      })

//      rdd.map(ExampleDataTransfer.IncrementTranslation(
//        KafkaStreamExample.tableFamily, KafkaStreamExample.tableQualifier))
//        .saveAsHadoopDataset(jobConf)
//
//      val hBaseConn = new HBaseConnection(KafkaStreamExample.rawDataTable,
//        KafkaStreamExample.zookeeper, List(KafkaStreamExample.tableFamily))
//      hBaseConn.openOrCreateTable()
//
//      val scan = new Scan()
//      scan.addFamily(KafkaStreamExample.tableFamilyValue)
//      val result = hBaseConn.scan(scan)
//      val resultList = HTableData.getTableData(result)
//
//      val brokerString = {
//        val stringBuilder = new StringBuilder
//        publishers.foreach(str => {
//          if (stringBuilder.nonEmpty) {
//            stringBuilder ++= ","
//          }
//          stringBuilder ++= str
//        })
//        stringBuilder.toString()
//      }
//      val simpleProducer = new SimpleKafkaProducer(brokerString, publishTopic)
//      //      println("---------")
//      simpleProducer.sendMessage("--------------------")
////      println("results:" + resultList.size)
//      resultList.foreach(data => {
//        val message = data.rowValue + ":" + data.getValue
////        println("published message:" + data.rowValue + ":" + data.getValue)
//        simpleProducer.sendMessage(message)
//      })
//      simpleProducer.close()
    }
    eachRDDProcessing
  }
}

object KafkaStreamExample{
  val zookeeper = "localhost:2181"
  val rawDataTable = "MyTestTable"
  val tableFamily = "f1"
  val tableQualifier = "c1"

  val ReceiverBasedApproach = 0
  val DirectApproach = 1

  def tableFamilyValue = Bytes.toBytes(tableFamily)
}
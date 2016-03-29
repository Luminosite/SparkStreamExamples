package com.paypal.risk.rds.KafkaStreamExample

import java.util.{Date, Properties}

import com.paypal.risk.rds.KafkaStreamExample.constant.KafkaConstant
import com.paypal.risk.rds.KafkaStreamExample.dataObject.YAMMessage
import com.paypal.risk.rds.KafkaStreamExample.parser.KafkaEventParser
import com.paypal.risk.rds.KafkaStreamExample.utils.DataTransfer
import com.paypal.user.{AccountVO, LoginSucceededMessage}
import com.paypal.vo.ValueObject
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.JavaConversions._


/**
  * Created by kufu on 28/1/16.
  */
class KafkaStreamExample {

  val logger = Logger.getLogger(KafkaStreamExample.getClass)
  def createStream(properties: Properties, brokerListKey: String, approachType: Int, ssc: StreamingContext):
  DStream[(String, String)] = {
    val kafkaPara = mutable.Map[String, String]()
    val list = properties.getProperty(brokerListKey)
    kafkaPara.put(KafkaConstant.BrokerList, list)

    for(key:String <- properties.stringPropertyNames()
        if KafkaConstant.ParaSet.contains(key) && nonEmptyProperty(properties, key)) {
      kafkaPara.put(key, properties.getProperty(key))
    }

    approachType match {
      case KafkaStreamExample.ReceiverBasedApproach =>
        val topicMap = Map(properties.getProperty(KafkaConstant.Topic)->1)
        if(nonEmptyProperty(properties, KafkaConstant.StreamNum)){
          val num = properties.getProperty(KafkaConstant.StreamNum).toInt
          val streams = (1 to num).map{_=>
            KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc,
              kafkaPara.toMap, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
          }
          ssc.union[(String, String)](streams)
        }else{
          KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc,
            kafkaPara.toMap, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
        }
      case KafkaStreamExample.DirectApproach =>
        val topicSet = Set[String](properties.getProperty(KafkaConstant.Topic))
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaPara.toMap, topicSet)
    }
  }

  def createStream(properties: Properties, suffix: Char, approachType: Int, ssc:StreamingContext):
  DStream[(String, String)] = {
    val brokerListKey = s"${KafkaConstant.BrokerListPrefix}$suffix"
    createStream(properties, brokerListKey, approachType, ssc)
  }

  def createStream(properties: Properties, approachType: Int, ssc:StreamingContext): DStream[(String, String)] = {
    createStream(properties, KafkaConstant.BrokerList, approachType, ssc)
  }

  def nonEmptyProperty(properties: Properties, key:String):Boolean ={
    properties.containsKey(key) && properties.getProperty(key).nonEmpty
  }

  def createStreams(properties: Properties, approachType: Int, ssc:StreamingContext): DStream[(String, String)] = {
    if(nonEmptyProperty(properties, KafkaConstant.SourceNum) &&
      properties.getProperty(KafkaConstant.SourceNum).toInt>1){
      println("To union")
      val num = properties.getProperty(KafkaConstant.SourceNum).toInt
      var stream:Option[DStream[(String, String)]]=None
      for(i <- 0 until num){
        stream match{
          case None => stream = Some(createStream(properties, ('a'+i).toChar, approachType, ssc))
          case Some(x) => stream = Some(x.union(createStream(properties, ('a'+i).toChar, approachType, ssc)))
        }
      }
      stream.get
    }else{
      createStream(properties, approachType, ssc)
    }
  }

  def run(approachType:Int, properties: Properties): Unit ={

    val conf = new SparkConf()
    conf.setAppName("KafkaStreamDemo")

    val ssc = new StreamingContext(conf, Seconds(2))

    val dataRDD:DStream[(String, String)] = createStreams(properties, approachType, ssc)

    val filteredData = dataRDD.filter(tuple=>KafkaEventParser.isInterestingMessage(tuple._2))

    filteredData.foreachRDD(genProcessing())

    ssc.start()
    ssc.awaitTermination()
  }

  def messageFilter(): Unit ={

  }

  def genProcessing()//, jobConf:JobConf)
  :(RDD[(String, String)])=>Unit = {
    def eachRDDProcessing(rdd:RDD[(String, String)]):Unit = {
      println("--------- a rdd: ----------")
      rdd.foreach(tuple => {
//        println(tuple._1 + ":" + tuple._2)
        val message = YAMMessage.fromString(tuple._2)
        val time = DataTransfer.getTimeMillis(s"${message.timePublished}")
        println(s"time c&p: ${message.timeConsumed} & ${message.timePublished}($time) \n\t" +
          s"message id & name & type: ${message.messageId} & ${message.messageName} ${message.messageType}")
        message.payload match {
          case vo:ValueObject =>
            println(s"\tvalue obj: ${message.payload}\n" +
            s"\tkeys:${vo.voFieldNames}\n" +
            s"\taccount:${vo.get("account")}")
            val account = vo.get("account").asInstanceOf[AccountVO]
            println(s"\taccount attr:${account.voFieldNames()}")
            println(s"\taccount attr:${account.get("encrypted_account_number")}:${account.get("time_created")}\n")
          case str:String => println(s"\tstr: ${message.payload}\n")
          case o=> println(s"\tnothing: ${message.payload}\n")
        }
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
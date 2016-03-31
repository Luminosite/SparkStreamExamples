package com.paypal.risk.rds.KafkaStreamExample

import java.math.BigInteger
import java.util
import java.util.Properties

import com.paypal.risk.rds.HBase.util.HBaseOperator
import com.paypal.risk.rds.KafkaStreamExample.constant.{KafkaConstant, MessageConstant}
import com.paypal.risk.rds.KafkaStreamExample.dataObject.{AccountInfo, YAMMessage}
import com.paypal.risk.rds.KafkaStreamExample.parser.KafkaEventParser
import com.paypal.risk.rds.KafkaStreamExample.utils.DataTransfer
import com.paypal.user.{LoginVO, AccountVO}
import com.paypal.vo.ValueObject
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * Created by kufu on 28/1/16.
  */
class KafkaStreamExample extends Serializable{

  //create kafka input DStream according to configuration
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

  //now there're broker.list.a and xx.xx.b so I make the key to get value from properties here.
  def createStream(properties: Properties, suffix: Char, approachType: Int, ssc:StreamingContext):
  DStream[(String, String)] = {
    val brokerListKey = s"${KafkaConstant.BrokerListPrefix}$suffix"
    createStream(properties, brokerListKey, approachType, ssc)
  }

  def createStream(properties: Properties, approachType: Int, ssc:StreamingContext): DStream[(String, String)] = {
    createStream(properties, KafkaConstant.BrokerList, approachType, ssc)
  }

  //small function to check properties
  def nonEmptyProperty(properties: Properties, key:String):Boolean ={
    properties.containsKey(key) && properties.getProperty(key).nonEmpty
  }

  //If there're multiple resources, union them.
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

  def calculateTime(): ((String, String)) => (Long, Long) = {
    def calclulate(tuple:(String, String)):(Long, Long) = {
      if (tuple != null && tuple._2!=null) {
        val message = YAMMessage.fromString(tuple._2)

        val publishTime = DataTransfer.getTimeMillis(s"${message.timePublished}")/1000
        message.payload match {
          case vo: ValueObject =>
            val account = vo.get("account").asInstanceOf[AccountVO]
            if(account!=null && account.get("time_created")!=null){
              val createTime = account.get("time_created").toString.toLong
              (publishTime-createTime, 1l)
            }else{
              (0l, 0l)
            }
//          case str: String => println(s"\tstr: ${message.payload}\n")
          case o => (0l, 0l)
        }
      }else{
        (0l, 0l)
      }
    }
    calclulate
  }

  def run(approachType:Int, kafkaProperties: Properties, hbaseProperties:Properties, interval:Int, period:Int): Unit ={

    val conf = new SparkConf()
    conf.setAppName("KafkaStreamDemo")

    val ssc = new StreamingContext(conf, Seconds(interval))

    val dataRDD:DStream[(String, String)] = createStreams(kafkaProperties, approachType, ssc)

    val filteredData = dataRDD.filter(tuple=>KafkaEventParser.isInterestingMessage(tuple._2))

//    filteredData.mapPartitions[(Long, Long)](genProcessing(hbaseProperties)).
//      reduce((a, b)=>(a._1+b._1, a._2+b._2)).foreachRDD(rdd=>{
//      println("--------- statistics ----------")
//      rdd.foreach(t=>{
//        println(s"${t._1}:${t._2}")
//      })
//    })

//    filteredData.foreachRDD(PrintProcessGen())
//    filteredData.flatMap[(BigInteger, Long)](message=>checkAccountEvent(message, kafkaProperties))

    filteredData.map[(Long, Long)](calculateTime()).reduce((a, b)=>(a._1+b._1, a._2+b._2))
      .foreachRDD(summaryTime(hbaseProperties, period:Int))

    ssc.start()
    ssc.awaitTermination()
  }


  def saveTime(time: String, t: (Long, Long), ho: HBaseOperator) = {
    ho.saveTime(time, t)
  }

  def scanTimes(startTimeStr: String, endTimeStr: String, ho: HBaseOperator): List[(Long, Long)] = {
    ho.getTimes(startTimeStr, endTimeStr)
  }

  def summaryTime(hbaseProperties:Properties, period:Int):(RDD[(Long, Long)])=>Unit = {
    def summary(rdd:RDD[(Long, Long)]): Unit = {
      println("-----statistics-----")
      rdd.foreach(f = t => {
        if(t._2!=0) {
//          val ret = t._1/t._2

          val timestamp:Long = System.currentTimeMillis()
          val timestampStr:String = timestamp.toString

          val ho = new HBaseOperator(hbaseProperties)

          saveTime(timestampStr, t, ho)

          val startTimeStr = (timestamp-period*1000).toString

          val list = scanTimes(startTimeStr, timestampStr, ho)
          var allTime:Long = t._1
          var allNum:Long = t._2

          list.foreach(tuple=>{
            allTime+=tuple._1
            allNum+=tuple._2
          })

          val time:java.lang.Double = allTime.toDouble
          val num:java.lang.Double = allNum.toDouble
          val avg:java.lang.Double = time / num
          val newAvg:java.lang.Double = new java.math.BigDecimal(avg)
            .setScale(2, java.math.BigDecimal.ROUND_HALF_UP).doubleValue()

          ho.saveStatistics(timestampStr, newAvg)

          println()
          println(s"record num:${list.size}")
          println(s"$allTime/$allNum")
          println("avg: " + newAvg)

          ho.close()
        }else{
          println("avg: 0")
        }
      })
    }
    summary
  }

  def checkAccountEvent(message: (String, String), kafkaProperties: Properties): Traversable[(BigInteger, Long)] = {

    mutable.Traversable()
  }

  def messageFilter(): Unit ={

  }

  def internalCheck(newAccounts: util.TreeMap[BigInteger, Long], loginedAccounts: util.TreeMap[BigInteger, Long]) = {
    val newIter = newAccounts.navigableKeySet().iterator()
    val loginIter = loginedAccounts.navigableKeySet().iterator()
    val ret = new ListBuffer[AccountInfo]()

    var compare:Int = 0
    var newValue = new BigInteger("0")
    var logValue = new BigInteger("0")

    while(newIter.hasNext && loginIter.hasNext){
      if(compare == 0){
        newValue = newIter.next()
        logValue = loginIter.next()
      }else if(compare>0) {
        logValue = loginIter.next()
      }else {
        newValue = newIter.next()
      }
      compare = newValue.compareTo(logValue)
      if(compare==0){
        ret+=new AccountInfo(newValue, newAccounts.get(newValue), loginedAccounts.get(newValue))
      }
    }
    ret
  }

  def searchRecord(loginedAccounts: util.TreeMap[BigInteger, Long], ho:HBaseOperator) = {
    val set:util.NavigableSet[BigInteger] = loginedAccounts.navigableKeySet()
    val list = ho.searchTemp(set)
    list.foreach(account=>{
      account.setLogin(loginedAccounts.get(account.id))
    })
    list
  }

  def deleteRecord(searchRecordResult: List[AccountInfo], ho: HBaseOperator) = {
    ho.deletes(searchRecordResult)
  }

  def addAccounts(accounts: List[AccountInfo], ho: HBaseOperator) = {
    ho.puts(accounts)
  }

  def addTempAccounts(newAccounts: util.TreeMap[BigInteger, Long], ho: HBaseOperator) = {
    ho.puts(newAccounts)
  }

  def genProcessing(hbaseProperties: Properties)//, jobConf:JobConf)
  :(Iterator[(String, String)])=>Iterator[(Long, Long)] = {
    def timeAccount(rdd:Iterator[(String, String)]):Iterator[(Long, Long)] = {
      val ho = new HBaseOperator(hbaseProperties)

      val newAccounts:util.TreeMap[BigInteger, Long] = new util.TreeMap[BigInteger, Long]()
      val loginedAccounts:util.TreeMap[BigInteger, Long] = new util.TreeMap[BigInteger, Long]()

      rdd.foreach(pair=>{
        if (pair != null) {
          if(pair._2.nonEmpty){
            val message = YAMMessage.fromString(pair._2)
            val account = message.payload.asInstanceOf[ValueObject]
              .get("account").asInstanceOf[AccountVO]
            if(account != null){
              val encryptedName = account.getAccountNumber
              val time = account.get("time_created").toString.toLong
              val timePublished = DataTransfer.getTimeMillis(s"${message.timePublished}")
              message.messageName match {
                case MessageConstant.Login_Succeeded => loginedAccounts.put(encryptedName, timePublished)
                case MessageConstant.New_Account => newAccounts.put(encryptedName, time)
                case default => println("Wrong Message: "+default)
              }
            }
          }
        }

      })

//      println(s"NewList size:${newAccounts.size()}, Login size:${loginedAccounts.size()}")

      val accounts:ListBuffer[AccountInfo] = internalCheck(newAccounts, loginedAccounts)

//      println(s"inner new login size:${accounts.size}")

      accounts.foreach(account=>{
        newAccounts.remove(account.id)
        loginedAccounts.remove(account.id)
      })

      val searchRecordResult:List[AccountInfo] = searchRecord(loginedAccounts, ho)

//      println(s"results searched from hbase size:${searchRecordResult.size}")

      deleteRecord(searchRecordResult, ho)

      accounts++=searchRecordResult

      val accountList = accounts.toList
      addAccounts(accountList, ho)

      println(s"all new log:${accountList.size}")

      addTempAccounts(newAccounts, ho)

//      println(s"all new create insert:${newAccounts.size}")

      ho.close()

      var allTime = 0l
      var num = 0l

      accountList.foreach(account=>{
        allTime+=(account.login-account.create)
        num+=1
      })

      List((allTime, num)).iterator
    }

    timeAccount

  }

  def PrintProcessGen()
  :(RDD[(String, String)])=>Unit = {
    def eachRDDProcessing(rdd:RDD[(String, String)]):Unit = {
      println("--------- a rdd: ----------")
      rdd.foreach(tuple => {
//        println(tuple._1 + ":" + tuple._2)
        val message = YAMMessage.fromString(tuple._2)

        val time = DataTransfer.getTimeMillis(s"${message.timePublished}")
        println(s"time c&p: ${message.timeConsumed} & ${message.timePublished}($time) \n\t" +
          s"message id & name & type: ${message.messageId} & ${message.messageName} & ${message.messageType}")
        message.payload match {
          case vo:ValueObject =>
            println(s"\tvalue obj: ${message.payload}\n" +
            s"\tkeys:${vo.voFieldNames}\n" +
              s"\tlogin:${vo.get("login")}\n" +
            s"\taccount:${vo.get("account")}")
            val login = vo.get("login").asInstanceOf[LoginVO]
            println(s"\tlogin attr:${login.voFieldNames()}")
            val account = vo.get("account").asInstanceOf[AccountVO]
            println(s"\taccount attr:${account.voFieldNames()}")
            println(s"\taccount attr:${account.getAccountNumber}:${account.get("time_created")}:${account.get("last_time_opened")}\n")
          case str:String => println(s"\tstr: ${message.payload}\n")
          case o=> println(s"\tnothing: ${message.payload}\n")
        }
      })

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
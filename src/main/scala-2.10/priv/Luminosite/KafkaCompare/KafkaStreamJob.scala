package priv.Luminosite.KafkaCompare

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import priv.Luminosite.HBase.util.HBaseConnection

/**
  * Created by kufu on 29/02/2016.
  */
class KafkaStreamJob extends Serializable{

  def createContext(approachType:Int,
                    consumeTopic:String,
                    zkOrBrokers:String,
                    checkpointDir:String
                   ): StreamingContext = {
    println("start a new context")
    val conf = new SparkConf()
    conf.setMaster("local[8]").setAppName("KafkaStreamExample")
      .setSparkHome("/home/kufu/spark/spark-1.5.2-bin-hadoop2.6")

    val threadNum = 3

    val ssc = new StreamingContext(conf, Seconds(2))

    val topicMap = Map(consumeTopic -> 1)

    val dataRDD:DStream[(String, String)] = approachType match {
      case KafkaStreamJob.ReceiverBasedApproach =>
        val dataRDDs:IndexedSeq[InputDStream[(String, String)]] = (1 to threadNum).map(i=>
          KafkaUtils.createStream(ssc, zkOrBrokers, "testKafkaGroupId", topicMap))
        ssc.union(dataRDDs)
      case KafkaStreamJob.DirectApproach =>
        ssc.checkpoint(checkpointDir)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, Map("metadata.broker.list" -> zkOrBrokers), Set[String](consumeTopic))
    }

    dataRDD.foreachRDD(genProcessing(approachType))
    ssc
  }

  def run(approachType:Int,
          consumeTopic:String,
          zkOrBrokers:String,
          publishTopic:String,
          publishers:List[String]
         ): Unit ={

    val checkpointDir = "hdfs://127.0.0.1:9000/user/kufu/KafkaApiComparisonCheckpoint"

    val ssc = StreamingContext.getOrCreate(checkpointDir, () => {
      createContext(approachType, consumeTopic, zkOrBrokers, checkpointDir)
    })

    ssc.start()
    ssc.awaitTermination()
  }

//  var n=0
  def genProcessing(approachType:Int):(RDD[(String, String)])=>Unit = {

    def eachRDDProcessing(rdd:RDD[(String, String)]):Unit = {
      println("--------- An RDD ---------")

//      if(n==0){
//        Thread.sleep(60000)
//        n+=1
//      }

      val family = approachType match{
        case KafkaStreamJob.DirectApproach => KafkaStreamJob.DirectFamily
        case KafkaStreamJob.ReceiverBasedApproach => KafkaStreamJob.NormalFamily
      }

      val families = KafkaStreamJob.DirectFamily :: KafkaStreamJob.NormalFamily :: Nil

      val time = System.currentTimeMillis().toString

      val messageCount = rdd.count()

      rdd.foreach(tuple => {
        println(tuple._2)
        val hBaseConn = new HBaseConnection(KafkaStreamJob.rawDataTable,
          KafkaStreamJob.zookeeper, families)
        hBaseConn.openOrCreateTable()
        val puts = new java.util.ArrayList[Put]()
        val strs = tuple._2.split(":")
        val row = strs(1) + ":" + strs(0) + ":" + time
        val put = new Put(Bytes.toBytes(row))
        put.add(Bytes.toBytes(family), Bytes.toBytes(KafkaStreamJob.tableQualifier),
          Bytes.toBytes("batch :" + strs(1)))
        puts.add(put)
        hBaseConn.puts(puts)
        hBaseConn.close()
      })

      println("--------- add "+messageCount+" messages ---------")
    }
    eachRDDProcessing
  }

}

object KafkaStreamJob{
  val DirectApproach = 0
  val ReceiverBasedApproach = 1

  val zookeeper = "localhost:2181"
  val rawDataTable = "KafkaCompareTable"
  val tableQualifier = "c1"
  val DirectFamily = "directF"
  val NormalFamily = "normalF"
}
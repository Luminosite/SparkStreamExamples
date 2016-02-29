package priv.Luminosite.KafkaCompare

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import priv.Luminosite.HBase.util.HBaseConnection

/**
  * Created by kufu on 29/02/2016.
  */
class KafkaStreamJob{

  def run(approachType:Int, consumeTopic:String, zkOrBrokers:String, publishTopic:String, publishers:List[String]): Unit ={

    val conf = new SparkConf()
    conf.setMaster("local[2]").setAppName("KafkaStreamExample")
      .setSparkHome("/home/kufu/spark/spark-1.5.2-bin-hadoop2.6")
      .setExecutorEnv("spark.executor.extraClassPath","target/scala-2.11/sparkstreamexamples_2.11-1.0.jar")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(conf, Seconds(2))
    val topicMap = Map(consumeTopic->3)

    val dataRDD:InputDStream[(String, String)] = approachType match {
      case KafkaStreamJob.ReceiverBasedApproach =>
        KafkaUtils.createStream(ssc, zkOrBrokers, "testKafkaGroupId", topicMap)
      case KafkaStreamJob.DirectApproach =>
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, Map[String, String]("metadata.broker.list" -> zkOrBrokers),
          Set[String](consumeTopic))
    }

    dataRDD.foreachRDD(genProcessing(approachType))

    ssc.start()
    ssc.awaitTermination()
  }

  val max = 5
  private var count = 0

  def genProcessing(approachType:Int):(RDD[(String, String)])=>Unit = {

    def eachRDDProcessing(rdd:RDD[(String, String)]):Unit = {
      if(count>max) throw new Exception("Stop here")
      println("--------- num: "+count+" ---------")

      val batchNum = count
      val curTime = System.currentTimeMillis()

      Thread.sleep(10000)

      val family = approachType match{
        case KafkaStreamJob.DirectApproach => KafkaStreamJob.DirectFamily
        case KafkaStreamJob.ReceiverBasedApproach => KafkaStreamJob.NormalFamily
      }

      val families = KafkaStreamJob.DirectFamily :: KafkaStreamJob.NormalFamily :: Nil

      val time = System.currentTimeMillis().toString

      val messageCount = rdd.count()

      rdd.foreach(tuple => {
        val hBaseConn = new HBaseConnection(KafkaStreamJob.rawDataTable,
          KafkaStreamJob.zookeeper, families)
        hBaseConn.openOrCreateTable()
        val puts = new java.util.ArrayList[Put]()
        val strs = tuple._2.split(":")
        val row = strs(1) + ":" + strs(0) + ":" + time
        val put = new Put(Bytes.toBytes(row))
        put.add(Bytes.toBytes(family), Bytes.toBytes(KafkaStreamJob.tableQualifier),
          Bytes.toBytes("batch " + batchNum.toString + ":" + strs(1)))
        puts.add(put)
        hBaseConn.puts(puts)
        hBaseConn.close()
      })

      count+=1
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
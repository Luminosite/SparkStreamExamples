package priv.Luminosite.KafkaStreamExampe

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import priv.Luminosite.HBase.util.{HTableData, HBaseConnection}
import priv.Luminosite.KafkaStreamExampe.OutputComponent.{ExampleDataTransfer, TableIncrementFormat}

/**
  * Created by kufu on 28/1/16.
  */
class KafkaStreamExample {

  def run(): Unit ={

    val conf = new SparkConf()
    conf.setMaster("local[2]").setAppName("KafkaStreamExample")
      .setSparkHome("/home/kufu/spark/spark-1.5.2-bin-hadoop2.6")
      .setExecutorEnv("spark.executor.extraClassPath","target/scala-2.11/sparkstreamexamples_2.11-1.0.jar")

    val ssc = new StreamingContext(conf, Seconds(2))

    val topicMap = Map("myTopic"->3)
    val wordCounts = KafkaUtils.createStream(ssc, "localhost:2181", "testKafkaGroupId", topicMap)
      .flatMap(_._2.split(" ")).map((_, 1)).reduceByKey(_+_)


    val hbaseConf = HBaseConfiguration.create()
    conf.set(TableIncrementFormat.OUTPUT_TABLE, KafkaStreamExample.rawDataTable)
    val jobConf:JobConf = new JobConf(hbaseConf, this.getClass)
    jobConf.setOutputFormat(classOf[TableIncrementFormat])
    jobConf.set(TableIncrementFormat.OUTPUT_TABLE, KafkaStreamExample.rawDataTable)

    wordCounts.foreachRDD(rdd=>{
      println("--------- a rdd: ----------")
      rdd.foreach(tuple=>{
        println(tuple._1+":"+tuple._2)
      })

      rdd.map(ExampleDataTransfer.IncrementTranslation(
        KafkaStreamExample.tableFamily, KafkaStreamExample.tableQualifier))
        .saveAsHadoopDataset(jobConf)

      val hBaseConn = new HBaseConnection(KafkaStreamExample.rawDataTable,
        KafkaStreamExample.zookeeper, List(KafkaStreamExample.tableFamily))
      hBaseConn.openOrCreateTable()

      val scan = new Scan()
      scan.addFamily(KafkaStreamExample.tableFamilyValue)
      val result = hBaseConn.scan(scan)
      val resultList = HTableData.getTableData(result)

      println("---------")
      resultList.foreach(data=>{
        println(data.rowValue+":"+data.getValue)
      })

    })

    ssc.start()
    ssc.awaitTermination()
  }

}

object KafkaStreamExample{
  val zookeeper = "localhost:2181"
  val rawDataTable = "MyTestTable"
  val tableFamily = "f1"
  val tableQualifier = "c1"

  def tableFamilyValue = Bytes.toBytes(tableFamily)
}
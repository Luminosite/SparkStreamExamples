package com.paypal.risk.rds.KafkaStreamExample

import java.io.{File, FileInputStream}
import java.util.Properties

import com.paypal.risk.rds.HBase.util.{HBaseConnection, HTableConnectionDemo}
import com.paypal.risk.rds.KafkaStreamExample.constant.KafkaConstant

/**
  * Created by kufu on 28/01/2016.
  */
object KafkaStreamExampleMain {

  type OptionMap = Map[Symbol, Any]

  def nextOption(map:OptionMap, list:List[String]):OptionMap = {
    list match{
      case Nil => map

      case "-kafkaOps"::value::tail => nextOption(map ++ Map('kafkaOps -> value), tail)

      case "-kafkaAPI--Direct"::tail =>
        nextOption(map ++ Map('kafkaAPI -> KafkaStreamExample.DirectApproach), tail)
      case "-kafkaAPI--Receiver"::tail =>
        nextOption(map ++ Map('kafkaAPI -> KafkaStreamExample.ReceiverBasedApproach), tail)

      case "-hbaseOps"::value::tail => nextOption(map ++ Map('hbaseOps -> value), tail)

      case option :: tail => println("Unknown opton:"+option)
        map
    }
  }

  def opsCheck(ops: OptionMap):Boolean = {
    if(ops.get('kafkaOps).nonEmpty){
      val file = new File(ops.get('kafkaOps).get.toString)
      if(!file.exists()) {
        println("Error: Kafka config file not exist!")
        return false
      }
    }else{
      println("Error: Kafka configuration hasn't been set!")
      return false
    }

    if(ops.get('hbaseOps).isEmpty){
      println("Error: HBase configuration hasn't been set!")
      return false
    }

    val file = new File(ops.get('hbaseOps).get.toString)
    if(!file.exists()) {
      println("Error: HBase config file not exist!")
      return false
    }

    true
  }

  def main(args: Array[String]) {
    val argList = args.toList
//    val argList = "-kafkaOps"::"/home/kufu/horton-input-kafka.conf"::Nil

    //
    val properties = new Properties
    var kafkaApproach = KafkaStreamExample.DirectApproach
    val hbaseProperties = new Properties

    //
    if(argList.nonEmpty){
      val ops = nextOption(Map(), argList)
      if(ops.nonEmpty){
        if(!opsCheck(ops)){
          return
        }
        //load kafka configuration
        val input = new FileInputStream(ops.get('kafkaOps).get.toString)
        properties.load(input)

        if(ops.get('kafkaAPI).nonEmpty){
          kafkaApproach = ops.get('kafkaAPI).get.asInstanceOf[Int]
        }

        //load hbase configuration
        val hbaseConfigInput = new FileInputStream(ops.get('hbaseOps).get.toString)
        hbaseProperties.load(hbaseConfigInput)

        val demo = new HTableConnectionDemo(hbaseProperties)
        demo.run()

//        new KafkaStreamExample().run(kafkaApproach, properties)
      }else{
        println("Some configuration is not correct")
      }
    }else{
      println("Some configuration is needed")
    }
  }
}

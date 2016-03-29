package com.paypal.risk.rds.HBase.util

import java.net.URL

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTableInterface, HConnectionManager}

/**
  * Created by kufu on 28/03/2016.
  */
object HTableFactory {

  def createHTable(tableName:String, hbaseConfig: Configuration):
  (HTableInterface, ()=>Unit) = {
    val connection = HConnectionManager.createConnection(hbaseConfig)
    val table = connection.getTable(tableName)

    def tableClose(): Unit = {
      connection.close()
      println("connection closed")
    }

    (table, tableClose)
  }

  def createHTable(tableName:String, configurationUrl:String): (HTableInterface, ()=>Unit) = {
    val config = HBaseConfiguration.create()
    config.addResource(new URL(configurationUrl))
    createHTable(tableName, config)
  }

}

package priv.Luminosite.HBase.util

import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HBaseAdmin, HTable}

/**
  * Created by kufu on 28/01/2016.
  */
class HBaseConnection(tableName:String, zookeeper:String, families:List[String]) {


  var table:HTable = null

  def openOrCreateTable(): Unit ={
    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", zookeeper)
    var admin:HBaseAdmin = null
    try{
      admin = new HBaseAdmin(config)
      if(!admin.tableExists(tableName)){
        val descriptor = new HTableDescriptor(TableName.valueOf(tableName))
        families.foreach(family=>descriptor.addFamily(new HColumnDescriptor(family)))
        admin.createTable(descriptor)
      }
      table = new HTable(config, tableName)
    }finally{
      admin.close()
    }
  }

  def put(put:Put): Unit ={
    if(table==null) throw new Exception("Open or create a table first.")
    table.put(put)
  }

  def close(): Unit ={
    table.close()
    table = null
  }
}

package com.paypal.risk.rds.HBase.util

import java.util.Properties

import com.paypal.risk.rds.KafkaStreamExample.constant.HBaseConstant
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by kufu on 28/03/2016.
  */
class HTableConnectionDemo(props:Properties) {
  def run(): Unit ={
    val connectionUrl = props.getProperty(HBaseConstant.configUrl)
    val tableName = props.getProperty(HBaseConstant.tableName)
    val mainFamily = props.getProperty(HBaseConstant.main_family)

    val table = HTableFactory.createHTable(tableName, connectionUrl)

    val scan = new Scan()
    scan.addColumn(Bytes.toBytes(mainFamily), Bytes.toBytes("c1"))
    val result = table._1.getScanner(scan)

    val results = HTableData.getTableData(result)
    results.foreach(table=>{
      println("value:"+table.getValue)
    })

    table._2.apply
  }
}

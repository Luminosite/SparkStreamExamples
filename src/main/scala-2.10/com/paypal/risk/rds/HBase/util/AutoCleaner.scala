package com.paypal.risk.rds.HBase.util

import com.paypal.risk.rds.KafkaCompare.KafkaStreamJob
import org.apache.hadoop.hbase.client.{Scan, Put}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by kufu on 29/02/2016.
  */
object AutoCleaner {

  def main (args: Array[String]) {
    try{
    clean(KafkaStreamJob.DirectApproach)
    } catch {
      case e:Exception=>e.printStackTrace()
    }
    try{
    clean(KafkaStreamJob.ReceiverBasedApproach)
    } catch {
      case e:Exception=>e.printStackTrace()
    }
  }

  def clean(approachType:Int): Unit ={
    val family = approachType match{
      case KafkaStreamJob.DirectApproach => KafkaStreamJob.DirectFamily
      case KafkaStreamJob.ReceiverBasedApproach => KafkaStreamJob.NormalFamily
    }

    val hBaseConn = new HBaseConnection(KafkaStreamJob.rawDataTable,
      KafkaStreamJob.zookeeper, List(family))
    hBaseConn.openOrCreateTable()

    val scan = new Scan()
    scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(KafkaStreamJob.tableQualifier))
    val result = hBaseConn.scan(scan)

    val results = HTableData.getTableData(result)

    val deletes = HTableData.genDeletes(results)

    hBaseConn.delete(deletes)

    hBaseConn.close()
  }

}

package com.paypal.risk.rds.HBase.util

import java.lang.Double
import java.math.BigInteger
import java.util
import java.util.Properties

import com.paypal.risk.rds.KafkaStreamExample.constant.HBaseConstant
import com.paypal.risk.rds.KafkaStreamExample.dataObject.AccountInfo
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * Created by kufu on 29/03/2016.
  */
class HBaseOperator(props:Properties) extends Serializable{

  def saveStatistics(time: String, newAvg: Double) = {
    val put = new Put(Bytes.toBytes(time))
    put.add(Bytes.toBytes(HBaseConstant.main_family),
      Bytes.toBytes(HBaseConstant.avg_time), Bytes.toBytes(newAvg))

    tableOperator._1.put(put)
  }

  def saveTime(time: String, t: (Long, Long)) = {

    val put = new Put(Bytes.toBytes(time))
    put.add(Bytes.toBytes(HBaseConstant.main_family),
      Bytes.toBytes(HBaseConstant.all_time), Bytes.toBytes(t._1))
    put.add(Bytes.toBytes(HBaseConstant.main_family),
      Bytes.toBytes(HBaseConstant.all_num), Bytes.toBytes(t._2))

    tableOperator._1.put(put)
  }

  def getTimes(start:String, end:String):List[(Long, Long)] = {
    val lists = new ListBuffer[(Long, Long)]
    val scan = new Scan(Bytes.toBytes(start), Bytes.toBytes(end))
    scan.addColumn(Bytes.toBytes(HBaseConstant.main_family), Bytes.toBytes(HBaseConstant.all_time))
    scan.addColumn(Bytes.toBytes(HBaseConstant.main_family), Bytes.toBytes(HBaseConstant.all_num))

    val scanner:ResultScanner = tableOperator._1.getScanner(scan)

    for(result <- scanner.iterator()){
      val time_cell:KeyValue = result.getColumn(Bytes.toBytes(HBaseConstant.main_family), Bytes.toBytes(HBaseConstant.all_time)).get(0)
      val allTime:Long = Bytes.toLong(time_cell.getValue)
      val num_cell:KeyValue = result.getColumn(Bytes.toBytes(HBaseConstant.main_family), Bytes.toBytes(HBaseConstant.all_num)).get(0)
      val allNum:Long = Bytes.toLong(num_cell.getValue)

      lists+=((allTime, allNum))
    }

    lists.toList
  }

  val tableOperator = {
    val connectionUrl = props.getProperty(HBaseConstant.configUrl)
    val tableName = props.getProperty(HBaseConstant.tableName)

    HTableFactory.createHTable(tableName, connectionUrl)
  }

  def searchTemp(set:util.NavigableSet[BigInteger]):List[AccountInfo] ={
    val getList = new ListBuffer[Get]
    set.foreach(row=>{
      val get = new Get(row.toByteArray)
      get.addColumn(Bytes.toBytes(HBaseConstant.temp_family), Bytes.toBytes(HBaseConstant.create_time))
      getList+=get
    })

    val results = tableOperator._1.get(getList)

    val list = new ListBuffer[AccountInfo]
    results.foreach(result=>{
      if(result.size()>1){
        val data = HTableData.getTableData(result)
        list+=new AccountInfo(new BigInteger(data.get(0).row), data.get(0).getLongValue(), 0l)
      }
    })
    list.toList
  }

  def puts(list: List[AccountInfo]): Unit ={
    val puts = new ListBuffer[Put]

    list.foreach(account=>{
      val put = new Put(account.id.toByteArray)
      put.add(Bytes.toBytes(HBaseConstant.main_family),
        Bytes.toBytes(HBaseConstant.create_time), Bytes.toBytes(account.create))
      put.add(Bytes.toBytes(HBaseConstant.main_family),
        Bytes.toBytes(HBaseConstant.login_time), Bytes.toBytes(account.login))
      puts+=put
    })
    tableOperator._1.put(puts.toList)
  }

  def puts(newAccounts: util.TreeMap[BigInteger, Long]) = {
    val puts = new ListBuffer[Put]

    newAccounts.keySet().foreach(row=>{
      val put = new Put(row.toByteArray)
      put.add(Bytes.toBytes(HBaseConstant.temp_family),
        Bytes.toBytes(HBaseConstant.create_time), Bytes.toBytes(newAccounts.get(row)))
      puts+=put
    })
    tableOperator._1.put(puts.toList)
  }

  def deletes(searchRecordResult: List[AccountInfo]) = {
    val deletes = new ListBuffer[Delete]

    searchRecordResult.foreach(account=>{
      val del = new Delete(account.id.toByteArray)
      del.deleteColumns(Bytes.toBytes(HBaseConstant.temp_family), Bytes.toBytes(HBaseConstant.create_time))
      deletes+=del
    })
    tableOperator._1.delete(deletes.toList)
  }

  def clean(): Unit = {
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes(HBaseConstant.main_family), Bytes.toBytes(HBaseConstant.create_time))
    scan.addColumn(Bytes.toBytes(HBaseConstant.main_family), Bytes.toBytes(HBaseConstant.login_time))
    scan.addColumn(Bytes.toBytes(HBaseConstant.main_family), Bytes.toBytes(HBaseConstant.all_time))
    scan.addColumn(Bytes.toBytes(HBaseConstant.main_family), Bytes.toBytes(HBaseConstant.all_num))
    scan.addColumn(Bytes.toBytes(HBaseConstant.main_family), Bytes.toBytes(HBaseConstant.avg_time))
    scan.addColumn(Bytes.toBytes(HBaseConstant.temp_family), Bytes.toBytes(HBaseConstant.create_time))

    val result:ResultScanner = tableOperator._1.getScanner(scan)

    val results = HTableData.getTableData(result)

    val deletes = HTableData.genDeletes(results)

    tableOperator._1.delete(deletes.toList)

  }

  def close(): Unit = {
    tableOperator._2.apply()
  }
}

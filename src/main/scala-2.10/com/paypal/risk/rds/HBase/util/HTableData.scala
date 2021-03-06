package com.paypal.risk.rds.HBase.util

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

/**
  * Created by kufu on 28/01/2016.
  */
case class HTableData(row:Array[Byte],
                 family:Array[Byte],
                 qualifier:Array[Byte],
                 timestamp:Long,
                 value:Array[Byte]) {

  def rowValue = Bytes.toString(row)
  def familyValue = Bytes.toString(family)
  def qualifierValue = Bytes.toString(qualifier)
  def getValue = {
    if (value.length == 8) {
      Bytes.toLong(value).toString
    } else {
      Bytes.toString(value)
    }
  }

  def getLongValue():Long = {
    if (value.length == 8) {
      Bytes.toLong(value)
    } else {
      0l
    }
  }

}

object HTableData{

  def genPut(data: HTableData): Put = {
    val put: Put = new Put(data.row, data.timestamp)
    put.add(data.family, data.qualifier, data.value)
    put
  }

  def getTableData(result: Result): List[HTableData] = {
    val retData: ListBuffer[HTableData] = new ListBuffer[HTableData]
    val row: Array[Byte] = result.getRow
    val familyMap = result.getMap
    familyMap.keySet.foreach(family=>{
      val qualifiers = familyMap.get(family)
      qualifiers.keySet.foreach(qualifier=>{
        val versions = qualifiers.get(qualifier)
        versions.navigableKeySet.foreach(timestamp=>{
          val value: Array[Byte] = versions.get(timestamp)
          val data: HTableData = new HTableData(row, family, qualifier, timestamp, value)
          retData.add(data)
        })
      })
    })
    retData.toList
  }
  def getTableData(result: ResultScanner): List[HTableData] = {
    val retList = new ListBuffer[HTableData]
    val iterator = result.iterator()
    while(iterator.hasNext){
      val result = iterator.next()
      retList++=getTableData(result)
    }
    retList.toList
  }

  def genGet(data: HTableData): Get = {
    val get: Get = new Get(data.row)
    get.addColumn(data.family, data.qualifier)
    get
  }

  def genDelete(data: HTableData): Delete = {
    val del = new Delete(data.row)
    del.deleteColumns(data.family, data.qualifier)
    del
  }

  def genDeletes(datum:List[HTableData]): List[Delete] = {
    val ret = new ListBuffer[Delete]
    datum.foreach(data=>{
      ret+=genDelete(data)
    })
    ret.toList
  }
}

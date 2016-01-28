package priv.Luminosite.HBase.util

import org.apache.hadoop.hbase.client.{Result, Get, Put}

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
    for (family <- familyMap.keySet) {
      for (qualifier <- familyMap.get(family).keySet) {
        import scala.collection.JavaConversions._
        for (l <- familyMap.get(family).get(qualifier).navigableKeySet) {
          val value: Array[Byte] = familyMap.get(family).get(qualifier).get(l)
          val data: HTableData = new HTableData(row, family, qualifier, l, value)
          retData.add(data)
        }
      }
    }
    retData.toList
  }

  def genGet(data: HTableData): Get = {
    val get: Get = new Get(data.row)
    get.addColumn(data.family, data.qualifier)
    get
  }
}

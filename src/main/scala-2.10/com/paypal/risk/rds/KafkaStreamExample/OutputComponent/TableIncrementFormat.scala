package com.paypal.risk.rds.KafkaStreamExample.OutputComponent

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Increment}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapred.{Reporter, RecordWriter, JobConf, FileOutputFormat}
import org.apache.hadoop.util.Progressable

/**
  * Created by kufu on 28/01/2016.
  */
class TableIncrementFormat extends FileOutputFormat[ImmutableBytesWritable, Increment]{
  override def getRecordWriter(ignored: FileSystem,
                               job: JobConf,
                               name: String,
                               progress: Progressable):
                              RecordWriter[ImmutableBytesWritable, Increment] = {

    val tableName = job.get(TableIncrementFormat.OUTPUT_TABLE)
    val hTable = new HTable(HBaseConfiguration.create(job), tableName)
    hTable.setAutoFlush(false, true)

    new IncrementRecordWriter(hTable)
  }
}

object TableIncrementFormat{
  val OUTPUT_TABLE: String = "hbase.mapred.outputtable"
}

class IncrementRecordWriter(hTable: HTable) extends RecordWriter[ImmutableBytesWritable, Increment]{

  override def write(key: ImmutableBytesWritable, value: Increment): Unit = {
    hTable.increment(value)
  }

  override def close(reporter: Reporter): Unit = {
    hTable.close()
  }
}
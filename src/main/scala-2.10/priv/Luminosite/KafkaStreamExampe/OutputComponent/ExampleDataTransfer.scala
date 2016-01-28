package priv.Luminosite.KafkaStreamExampe.OutputComponent

import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by kufu on 28/01/2016.
  */
object ExampleDataTransfer {

  def IncrementTranslation(family:String, qualifier:String) :
    ((String, Int)) => (ImmutableBytesWritable, Increment) = {
    def retFunc(tuple:(String, Int)):(ImmutableBytesWritable, Increment)= {
      val rowValue = Bytes.toBytes(tuple._1)
      val increment = new Increment(rowValue)
      increment.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), tuple._2.toLong)
      (new ImmutableBytesWritable(rowValue), increment)
    }
    retFunc
  }

}

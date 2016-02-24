package priv.Luminosite.KafkaStreamExampe.OutputComponent

import java.util.Properties

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}

/**
  * Created by kufu on 24/02/2016.
  */
class SimpleKafkaProducer(brokers:String, topic:String) {

  private val producer = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }

  private var num:Int = 0

  def sendMessage(message:String) = {
    producer.send(new ProducerRecord[String, String](topic, num.toString, message))
    num+=1
  }

  def close()={
    producer.close()
  }
}

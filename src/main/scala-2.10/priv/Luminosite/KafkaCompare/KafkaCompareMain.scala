package priv.Luminosite.KafkaCompare

/**
  * Created by kufu on 29/02/2016.
  */
object KafkaCompareMain {

  def main(args: Array[String]) {
    val flag = KafkaStreamJob.ReceiverBasedApproach

    val publishTopic = "publish"
    val publishBrokers = "localhost:9092" :: Nil
    val consumeTopic = "myTopic"
    val consumeZookeeper = "localhost:2181"
    val consumeBroker = "localhost:9093"

    println("start")

    flag match {
      case KafkaStreamJob.ReceiverBasedApproach=>
        new KafkaStreamJob().run(KafkaStreamJob.ReceiverBasedApproach,
          consumeTopic, consumeZookeeper, publishTopic, publishBrokers)
      case KafkaStreamJob.DirectApproach=>
        new KafkaStreamJob().run(KafkaStreamJob.DirectApproach,
          consumeTopic, consumeBroker, publishTopic, publishBrokers)
    }
  }

}

package priv.Luminosite.KafkaStreamExampe

/**
  * Created by kufu on 28/01/2016.
  */
object KafkaStreamExampleMain {
  def main(args: Array[String]) {
    val publishTopic = "publish"
    val publishBrokers = "localhost:9095" :: Nil
    val consumeTopic = "myTopic"
    val consumeZookeeper = "localhost:2181"
    val consumeBroker = "localhost:9095"
//    new KafkaStreamExample().run(KafkaStreamExample.ReceiverBasedApproach,
//      consumeTopic, consumeZookeeper, publishTopic, publishBrokers)
    new KafkaStreamExample().run(KafkaStreamExample.DirectApproach,
      consumeTopic, consumeBroker, publishTopic, publishBrokers)
  }
}

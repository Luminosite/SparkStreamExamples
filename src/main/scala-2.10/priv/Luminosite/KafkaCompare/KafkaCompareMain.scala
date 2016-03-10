package priv.Luminosite.KafkaCompare

/**
  * Created by kufu on 29/02/2016.
  */
object KafkaCompareMain {

  def main(args: Array[String]) {

    var flag = KafkaStreamJob.DirectApproach

    if(args.length>0){
      if(args(0).equalsIgnoreCase("-d")){
        flag = KafkaStreamJob.DirectApproach
        println("using direct api")
      }else if(args(0).equalsIgnoreCase("-r")){
        flag = KafkaStreamJob.ReceiverBasedApproach
        println("using receiver-based api")

      }
    }

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

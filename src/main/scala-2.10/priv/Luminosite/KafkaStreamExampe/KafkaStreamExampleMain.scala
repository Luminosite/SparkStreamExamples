package priv.Luminosite.KafkaStreamExampe

/**
  * Created by kufu on 28/01/2016.
  */
object KafkaStreamExampleMain {
  def main(args: Array[String]) {
    val publishTopic = "publish"
    val publishBrokers = "localhost:9094" :: Nil
    new KafkaStreamExample().run(publishTopic, publishBrokers)
  }
}

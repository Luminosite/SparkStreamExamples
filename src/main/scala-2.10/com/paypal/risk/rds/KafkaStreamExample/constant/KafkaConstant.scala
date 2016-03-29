package com.paypal.risk.rds.KafkaStreamExample.constant

/**
  * Created by kufu on 23/03/2016.
  */
object KafkaConstant {
  def SourceNum = "sourceNum"
  def StreamNum = "streamNum"
  def BrokerListPrefix = "metadata.broker.list."
  def BrokerList = "metadata.broker.list"
  def Topic = "topic"

  def ParaSet = Set(
    "zookeeper.connect",
    "refresh.leader.backoff.ms",
    "zookeeper.session.timeout.ms"
  )
}

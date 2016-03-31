package com.paypal.risk.rds.KafkaStreamExample.dataObject

import java.io.ByteArrayInputStream

import com.paypal.vo.ValueObject
import com.paypal.vo.serialization.UniversalDeserializer

/**
 * Created by jianshuang in 27/10/2014.
 */

case class YAMMessage(jMSQueueName: String, jMSMessageName: String, messageName: String, messageId: String,
                      messageType: String, serviceName: String, timePublished: String, timeConsumed: String,
                      currentPage: String, totalPages: String, appMetadata: String, payload: Any) {

  def toMap: Map[String, Any] = {
    Map(
      "jms_queue_name" -> jMSQueueName,
      "jms_message_name" -> jMSMessageName,
      "message_name" -> messageName,
      "message_id" -> messageId,
      "message_type" -> messageType,
      "service_name" -> serviceName,
      "time_published" -> timePublished,
      "time_consumed" -> timeConsumed,
      "current_page" -> currentPage,
      "total_pages" -> totalPages,
      "app_metadata" -> appMetadata,
      "payload" -> (payload match {
        case p: ValueObject => new ValueObjectMapWrapper(p).asInstanceOf[Any]
        case p: String => p.asInstanceOf[Any]
      })
    )
  }

}

object YAMMessage {
  def fromString(msg: String, delimiter: Char = '\u0010'): YAMMessage = {
    val a = msg.split(delimiter)

    if (a.length != 12)
      throw sys.error(s"Wrong message: $msg")

    val voStr = a(11)

    try{
      val deser = new UniversalDeserializer()
      val payload =
        if (voStr.startsWith("U30300"))
          voStr.replaceFirst("U30300", "U3\u0003\u0000")
        else if (voStr.startsWith("U80000"))
          voStr.replaceFirst("U80000", "U8\u0000\u0000")
        else
          throw new RuntimeException(s"Payload cannot be parsed: $voStr")
      val vo = deser.deserialize(new ByteArrayInputStream(payload.getBytes))

      YAMMessage(a(0), a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8), a(9), a(10), vo)
    }catch {
      case e:Throwable => YAMMessage(a(0), a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8), a(9), a(10), voStr.substring(6))
    }

  }

}
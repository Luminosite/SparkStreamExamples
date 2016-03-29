package com.paypal.risk.rds.KafkaStreamExample.utils

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by kufu on 28/03/2016.
  */
object DataTransfer {

  def getTimeMillis(dateString: String) : Long = {
    val dateFormat = new SimpleDateFormat("yyy-MM-dd HH:mm:ss")
    try{
      val date:Date = dateFormat.parse(dateString)
      date.getTime
    } catch {
      case e:Exception => e.printStackTrace()
        -1l
    }
  }

}

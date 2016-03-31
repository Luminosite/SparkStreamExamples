package com.paypal.risk.rds.KafkaStreamExample.dataObject

import java.math.BigInteger

/**
  * Created by kufu on 29/03/2016.
  */
class AccountInfo(n:BigInteger, c:Long, l:Long) {
  var login = l
  var create = c
  var id = n
  def setLogin(t:Long):Unit = {
    login = t
  }
}

package com.paypal.risk.rds.KafkaStreamExample.parser

/**
  * Created by kufu on 28/03/2016.
  */
class KafkaEventParser {



}

object KafkaEventParser{

  private def  isNewAccountCreatedMessage(msg: String): Boolean =
    msg.startsWith("topic://default.User_NewAccountCreated")

  private def isAddCCMessage(msg: String): Boolean =
    msg.startsWith("topic://default.idicreditcardcreate2eventservice")

  private def isEmailChangedMessage(msg: String): Boolean =
    msg.startsWith("topic://User.EmailChanged")

  private def isPhoneChangedMessage(msg: String): Boolean =
    msg.startsWith("topic://User.PhoneChanged")

  private def isAddressChangedMessage(msg: String): Boolean =
    msg.startsWith("topic://User.AddressChanged")

  private def isAccountSuccessLoginMessage(msg: String): Boolean =
    msg.startsWith("topic://default.User_LoginSucceeded")

  def isInterestingMessage(msg: String): Boolean = {
//    isNewAccountCreatedMessage(msg) ||
      isAccountSuccessLoginMessage(msg)
  }

}
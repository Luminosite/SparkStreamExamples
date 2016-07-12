package com.paypal.risk.rds.hiveExamples

/**
  * Created by kufu on 07/07/2016.
  */
object HiveDataIO extends App with sqlContextHelper{
  val appName = "HiveDataIO_Example"
//  val sqlContext = getHiveContext(appName)

  {
    val sc = getSparkContext(appName)



    sc.stop()
  }

}

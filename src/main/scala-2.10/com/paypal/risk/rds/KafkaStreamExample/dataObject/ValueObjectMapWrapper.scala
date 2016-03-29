package com.paypal.risk.rds.KafkaStreamExample.dataObject

import scala.collection.JavaConverters._

import com.paypal.vo.ValueObject

class ValueObjectMapWrapper(vo: ValueObject) extends Map[String, Any] {

  private def mapify(vo: Any) = {
    vo match {
      case v: ValueObject => new ValueObjectMapWrapper(v)
      case v: com.paypal.types.Currency => Map("amount" -> v.getAmount, "unit_amount" -> v.getUnitAmount, "currency_code" -> v.getCurrencyCode)
      case v: java.util.List[Any] =>
        v.asScala.toList.map {
          case vo: ValueObject => new ValueObjectMapWrapper(vo)
          case notVO => notVO
        }
      case v: Seq[Any] => v.map {
        case vo: ValueObject => new ValueObjectMapWrapper(vo)
        case notVO => notVO
      }
      case v => v
    }
  }

  override def get(key: String): Option[Any] = {
    val value = mapify(vo.get(key))
    Option(value)
  }

  override def iterator: Iterator[(String, Any)] = {
    val iter = vo.voFieldNames().iterator()

    new Iterator[(String, Any)] {
      override def next(): (String, Any) = {
        val key = iter.next()
        val value = mapify(vo.get(key))
        (key, value)
      }

      override def hasNext: Boolean = {
        iter.hasNext
      }
    }
  }

  override def -(key: String): Map[String, Any] = {
    vo.set(key, null)
    this
  }

  override def +[B1 >: Any](kv: (String, B1)): Map[String, B1] = {
    val DEFAULT_QUALIFIER: String = "public"
    vo.addFieldQualifier(kv._1, DEFAULT_QUALIFIER, kv._2.toString)
    this
  }

}

object ValueObjectMapWrapper {

  def handleFlattenMap(m: ValueObjectMapWrapper, prefix: String = "", sep: String = ".")(func: (String, String) => _): Unit = {
    // assume all Maps' key is String
    m foreach {
      case (key: String, value: Seq[Any]) =>
        value.zipWithIndex foreach {
          case (v: ValueObjectMapWrapper, index) =>
            handleFlattenMap(v, prefix=mkKey(key, prefix, sep)+s"[$index]", sep=sep)(func)
          case (v, index) =>
            func(mkKey(key, prefix, sep) + s"[$index]", if (value == null) "" else value.toString)
        }

      case (key: String, value: ValueObjectMapWrapper) =>
        handleFlattenMap(value, prefix=mkKey(key, prefix, sep), sep=sep)(func)

      case (key, value) =>
        func(mkKey(key, prefix, sep), if (value == null) "" else value.toString)
    }

    def mkKey(key: String, prefix: String, sep: String): String =
      if (prefix.trim.isEmpty) key else s"$prefix$sep$key"
  }

  object Implicit {

    import scala.language.implicitConversions

    implicit def valueObjectToMap(vo: ValueObject): Map[String, Any] = {
      new ValueObjectMapWrapper(vo)
    }

  }

}


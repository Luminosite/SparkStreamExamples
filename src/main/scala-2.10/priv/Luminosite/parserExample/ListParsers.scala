package priv.Luminosite.parserExample

import scala.util.parsing.combinator._

/**
  * Created by kufu on 17/03/2016.
  */
class ListParsers extends Parsers{
  def word:Parser[String] = """[a-z]+""".r ^^ {_.toString}
}

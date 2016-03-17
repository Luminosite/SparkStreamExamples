package priv.Luminosite.parserExample

/**
  * Created by kufu on 17/03/2016.
  */
object ParserMain {
  def main(args: Array[String]): Unit = {
    RegexParsers#parse[String]((new ListParsers).word, "work a")
  }
}

package menthor.config

import scala.util.parsing.combinator.RegexParsers
import scala.util.parsing.input.Reader
import java.io.{Reader => JReader}

object WorkerModifier extends Enumeration {
  val Absolute = Value
  val Times = Value
  val Plus = Value
}

class ConfigParser extends RegexParsers {
  override val whiteSpace = """(\s+|#[^\n]*\n)+""".r

  // Tokens
  val ipToken: Parser[String] = """[0-9a-fA-F:.]+""".r
  val hostnameToken: Parser[String] = """[-A-Za-z0-9.]+""".r
  val portToken: Parser[String] = """\d+""".r
  val workerModToken: Parser[String] = """[+*]?""".r
  val workerToken: Parser[String] = """\d+""".r

  def hostname = "[" ~> (ipToken ||| hostnameToken) <~ "]"

  def port = (":" ~> portToken) ^^ { result =>
    result.toInt
  }

  def workerMod = (workerModToken ?) ^? {
    case Some("+") => WorkerModifier.Plus
    case Some("*") => WorkerModifier.Times
    case None => WorkerModifier.Absolute
  }

  def worker = workerMod ~ {
    workerToken ^^ { result =>
      result.toInt
    }
  }

  def config = entry *

  def entry = (hostname ~ ((port ?) ~ (worker ?))) ^? {
    case ~(hostname, ~(port, worker)) => (hostname, port, worker)
  }
}

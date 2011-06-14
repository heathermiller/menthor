package pagerank

import scala.util.parsing.combinator.RegexParsers

object PageRankGraphParser extends RegexParsers {
  override val whiteSpace = """[\s&&[^\r\n]]""".r

  val vidToken: Parser[String] = """\d+""".r
  val eol: Parser[String] = """\r?\n""".r

  def vid = vidToken ^^ { result => result.toInt }
  def head = vid <~ ":"
  def successors = rep(vid)
  def vertex = head ~ successors ^? { case ~(head, successors) => (head, successors) }
  def vertexLine = vertex <~ eol
}

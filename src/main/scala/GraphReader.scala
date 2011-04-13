package processing
package parallel

import scala.collection.mutable.{Map, HashMap}
import scala.collection.mutable.ListBuffer

import scala.util.parsing.combinator._
import scala.util.parsing.input.{ Reader }
import scala.util.parsing.input.CharArrayReader.EofCh

import scala.collection.parallel.immutable.ParVector

object GraphReader extends RegexParsers {
  override def skipWhitespace = false

  lazy val token: Parser[String] =
    """\S+""".r
  lazy val edgeline: Parser[List[String]] =
    repsep(token, whiteSpace)

  val vertices: Map[String, Vertex[Double]] = new HashMap[String, Vertex[Double]]

  def tokenize(line: String): List[String] =
    tokenize(line, x => throw new Exception(x))

  def tokenize(line: String, onError: String => Unit): List[String] =
    parse(edgeline, line.trim) match {
      case Success(args, _)     => args
      case NoSuccess(msg, rest) => onError(msg); List()
    }

  def readGraph(lines: Iterator[String]): Graph[Double] = {
    val graph = new Graph[Double]

    for (line <- lines) {
      val labels = tokenize(line)
      //println("read labels " + labels)

      val firstLabel = labels.head.substring(0, labels.head.length - 1)
      val firstVertexOpt = vertices.get(firstLabel)
      val firstVertex =
        if (firstVertexOpt.isEmpty) graph.addVertex(new PageRankVertex(firstLabel))
        else firstVertexOpt.get
      vertices.put(firstLabel, firstVertex)

      for (targetLabel <- labels.tail) {
        val vertexOpt = vertices.get(targetLabel)
        val targetVertex = if (vertexOpt.isEmpty) {
          val newVertex = graph.addVertex(new PageRankVertex(targetLabel))
          vertices.put(targetLabel, newVertex)
          newVertex
        } else
          vertexOpt.get

        firstVertex.connectTo(targetVertex)
      }
    }
    graph
  }

  def readParVector(lines: Iterator[String]): ParVector[Vertex[Double]] = {
    val graph = readGraph(lines)
    (new ParVector) ++ graph.vertices
  }

  def printGraph(g: Graph[Double]) {
    for (v <- g.vertices) {
      print(v.label + ":")
      for (to <- v.neighbors) {
        print(" " + to.label)
      }
      println()
    }
  }
}

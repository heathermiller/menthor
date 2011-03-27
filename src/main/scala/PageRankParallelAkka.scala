package menthor.akka

import scala.collection.mutable.{Map, HashMap}

import akka.actor
import actor.ActorRef
import actor.Actor.actorOf

class PageRankVertex(label: String) extends Vertex[Double](label, 0.0d) {

  def numVertices = graph.vertices.size

  def update(superstep: Int, incoming: List[Message[Double]]): Substep[Double] = {

    if (superstep >= 1) {
      var sum = 0.0d
      for (msg <- incoming) {
        sum += msg.value
      }
      value = (0.15 / numVertices) + 0.85 * sum
    }

    if (superstep < 30) {
      val n = neighbors.size
      for (neighbor <- neighbors) yield {
        Message(this, neighbor, value / n)
      }
    } else {
      List()
    }

  }
}

object PageRank {

  def runPageRank(iterations: Int) {
    println("running PageRank...")
    println("Creating new graph...")
    var g: Graph[Double] = null
    val ga = actorOf({ g = new Graph[Double]; g })
    
    // a little web graph: BBC -> MS, EPFL -> BBC, PHILIPP -> BBC, PHILIPP -> EPFL
    val d1 = g.addVertex(new PageRankVertex("BBC"))
    val d2 = g.addVertex(new PageRankVertex("MS"))
    val d3 = g.addVertex(new PageRankVertex("EPFL"))
    val d4 = g.addVertex(new PageRankVertex("PHILIPP"))
    d1.connectTo(d2)
    d3.connectTo(d1)
    d4.connectTo(d1)
    d4.connectTo(d3)

    g.synchronized {
      for (v <- g.vertices) {
        v.value = 0.25d
      }
    }

    println("values before propagation:")
    g.synchronized {
      for (v <- g.vertices) {
        println(v.label + ": " + v.value)
      }
    }

    ga.start()
    g.iterate(iterations)

    println("values after propagation:")
    g.synchronized {
      for (v <- g.vertices) {
        println(v.label + ": " + v.value)
      }
    }

    g.terminate()
  }

  import java.io.{FileWriter, PrintWriter}

  def runWikipediaRank(numIterations: Int, dataDir: String, numPages: Int, small: Boolean) {
    println("Reading wikipedia graph from file...")
    //val lines = scala.io.Source.fromFile("testgraph.txt").getLines()

    val linesOrig = scala.io.Source.fromFile(dataDir + (if (small) "links-sorted-small.txt" else "links-simple-sorted.txt")).getLines()
    val lines = if (small)
      linesOrig.take(numPages)
    else {
      // drop first 150'000 entries
      linesOrig.drop(150000).take(numPages)
    }
    val (wikigraph, ga) = GraphReader.readGraph(lines)
    //GraphReader.printGraph(wikigraph)
    println("#vertices: " + wikigraph.vertices.size)

    println("Building page title map...")
    val names: Map[String, String] = new HashMap[String, String] {
      override def default(label: String) = {
        "no_title[" + label + "]"
      }
    }
    
    val titles = scala.io.Source.fromFile(dataDir + "titles-sorted-small.txt").getLines()
    for ((title, i) <- titles.take(400000) zipWithIndex)
      names.put("" + i, title)

    ga.start()
    wikigraph.iterate(numIterations)

    wikigraph.synchronized {
      val sorted = wikigraph.vertices.sortWith((v1: Vertex[Double], v2: Vertex[Double]) => v1.value > v2.value)
      for (page <- sorted.take(10)) {
        println(names(page.label) + " has rank " + page.value)
      }
    }

    wikigraph.terminate()
  }

  def main(args: Array[String]) {
    runWikipediaRank(args(0).toInt, args(1), args(2).toInt, true) // numPages = 20'000
    //runPageRank(args(0).toInt)
  }

}

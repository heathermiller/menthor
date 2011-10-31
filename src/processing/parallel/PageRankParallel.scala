package processing.parallel

import scala.collection.mutable.{Map, HashMap}
import benchmark.TicToc

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

object PageRank extends TicToc{

  def runPageRank(iterations: Int) {
    println("running PageRank...")
    println("Creating new graph...")
    val g = new Graph[Double]

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

    g.start()
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

    tic
    val linesOrig = scala.io.Source.fromFile(dataDir + "links-sorted.txt" ).getLines()
    val lines = if (small)
      linesOrig.take(numPages)
    else {
      // drop first 150'000 entries
      linesOrig.drop(150000).take(numPages)
    }
    val wikigraph = GraphReader.readGraph(lines)
    //GraphReader.printGraph(wikigraph)
    println("#vertices: " + wikigraph.vertices.size)

    
    println("Building page title map...")
    val names: Map[String, String] = new HashMap[String, String] {
      override def default(label: String) = {
        "no_title[" + label + "]"
      }
    }
    
    val titles = scala.io.Source.fromFile(dataDir + "titles-sorted.txt").getLines()
    for ((title, i) <- titles.take(400000) zipWithIndex)
      names.put("" + i, title)
    toc("I/O")
      
    
    tic
    wikigraph.start()
    wikigraph.iterate(numIterations)
    toc("comp")
    
    tic
    wikigraph.synchronized {
      val sorted = wikigraph.vertices.sortWith((v1: Vertex[Double], v2: Vertex[Double]) => v1.value > v2.value)
      for (page <- sorted.take(10)) {
        println(names(page.label) + " has rank " + page.value)
      }
    }

    wikigraph.terminate()
    toc("clean")
    
    writeTimesLog("bench/PageRankParallelBasic_"+numPages)
    println()
    printTimesLog
  }

  def main(args: Array[String]) {
    runWikipediaRank(args(0).toInt, args(1), args(2).toInt, true) // numPages = 20'000
    //runPageRank(args(0).toInt)
  }

}

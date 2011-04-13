package processing.parallel

import java.io.{FileWriter, PrintWriter}
import scala.collection.mutable.{Map, HashMap}

object PageRankPar {

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
//    val wikigraph = GraphReader.readGraph(lines)
    val vec = GraphReader.readParVector(lines)

    //GraphReader.printGraph(wikigraph)
    println("#vertices: " + vec.size)

    println("Building page title map...")
    val names: Map[String, String] = new HashMap[String, String] {
      override def default(label: String) = {
        "no_title[" + label + "]"
      }
    }
    
    val titles = scala.io.Source.fromFile(dataDir + "titles-sorted-small.txt").getLines()
    for ((title, i) <- titles.take(400000) zipWithIndex)
      names.put("" + i, title)

//    wikigraph.start()
//    wikigraph.iterate(numIterations)
/*
    wikigraph.synchronized {
      val sorted = wikigraph.vertices.sortWith((v1: Vertex[Double], v2: Vertex[Double]) => v1.value > v2.value)
      for (page <- sorted.take(10)) {
        println(names(page.label) + " has rank " + page.value)
      }
    }

    wikigraph.terminate()
*/

    def pageRank(v: Vertex[Double], superstep: Int) {
      if (superstep >= 1) {
        var sum = 0.0d
        for (in <- v.ins) {
          sum += (in.prev / in.neighbors.size)
        }
        v.value = (0.15 / /*numVertices*/vec.size) + 0.85 * sum
      }
    }

    //todo: enable messages
    //wait: prev and value must be volatile only for parcoll version!
    var superstep = 0
    val vecseq = vec.seq
    vecseq.foreach { url => url.prev = 0.5 }
    while (/*superstep < 30*/ !vecseq.forall(url => (url.prev - url.value).abs < 0.000000001)) {
      superstep += 1
      if (superstep > 30)
        println("hoi")
      vecseq.foreach { url => url.prev = url.value }
      vecseq.foreach { url => pageRank(url, superstep) }
    }

    vecseq.synchronized {
      val sorted = vecseq.toList.sortWith((v1: Vertex[Double], v2: Vertex[Double]) => v1.value > v2.value)
      for (page <- sorted.take(10)) {
        println(names(page.label) + " has rank " + page.value)
      }
    }
    
  }

  def main(args: Array[String]) {
    runWikipediaRank(args(0).toInt, args(1), args(2).toInt, true) // numPages = 20'000
    //runPageRank(args(0).toInt)
  }

}

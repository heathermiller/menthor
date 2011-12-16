package menthor.akka

import scala.collection.mutable.{ Map, HashMap }
import akka.actor
import actor.ActorRef
import actor.Actor.actorOf
import benchmark.TicToc
import scala.util.Sorting

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

object PageRank extends TicToc {

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

  import java.io.{ FileWriter, PrintWriter }

  /**
   * Run page rank on given data.
   * @param numIterations Number of iterations of the algorithm (30 is a good number to test).
   * @param dataDir Directory where the data can be found (try data/ to test).
   * @param numPages Number of pages to read in (try 1000-3000 to test).
   */
  def runWikipediaRank(numIterations: Int, dataDir: String, numPages: Int) {
    println("Reading wikipedia graph from file...")

    tic

    val linesOrig = scala.io.Source.fromFile(dataDir + "links-sorted.txt").getLines()
    val lines = linesOrig.take(numPages)
    val (wikigraph, ga) = GraphReader.readGraph(lines)
    //GraphReader.printGraph(wikigraph)

    /**
     * =====================================
     * Change the Graph Operation Mode here!
     * =====================================
     * Default for use with Parallel 
     * Pollections is the SingleWorkerMode.
     * =====================================
     */
    wikigraph.setOpMode(SingleWorkerMode)
    //        wikigraph.setOpMode(MultiWorkerMode)
    //    wikigraph.setOpMode(FixedWorkerMode(1))
    //    wikigraph.setOpMode(IAmLegionMode)

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

    ga.start()
    wikigraph.iterate(numIterations)

    toc("comp")
    tic

    wikigraph.synchronized {
      /*
       * The toList() call is expensive, however this is not measured as time
       * spent on computation and therefore we don't care. To fix this, write
       * a sorting algorithm which relies purely on API provided by GenSeq.
       * Doing this is about as fun as typing in the dark on an upside down
       * keyboard using only your feet.
       */
      val sorted = wikigraph.vertices.toList.sortWith((v1: Vertex[Double], v2: Vertex[Double]) => v1.value > v2.value)
      for (page <- sorted.take(10)) {
        println(names(page.label) + " has rank " + page.value)
      }
    }

    wikigraph.terminate()
    toc("clean")

    writeTimesLog("bench/AA_PC_1_" + numPages)
    println()
    printTimesLog

  }

  def main(args: Array[String]) {
    if (args.length == 3) {
      // Default: use all processors for parallel collections... 
      runWikipediaRank(args(0).toInt, args(1), args(2).toInt)
    } else if (args.length >= 4) {
      // Use this for benchmarking/testing only...
      println("----------------------------------------------------------")
      println("Overriding Parallel Collections parallelism: " + args(3).toInt)
      println("----------------------------------------------------------")
      // Set the number of cores to use for parallel collections
      scala.collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(args(3).toInt)
      runWikipediaRank(args(0).toInt, args(1), args(2).toInt)
    } else {
      println("Usage: args[String] = <#iterations> <data_dir> <#pages> (<num_procs>)")
    }
  }

}

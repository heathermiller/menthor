package processing.parallel

import math.min
import scala.collection.mutable.Queue

class ShortestPathVertex(label: String, isSource: Boolean) extends
    Vertex[Double](label, Double.PositiveInfinity) {

  def this(label: String) = this(label, false)

  override def update() = mkSubstep {
    val dists = Iterator(value) ++ incoming.iterator.map(msg => msg.value)
    val mindist = if (isSource) 0.0 else dists.min
    if (mindist < value) {
      value = mindist
      for (neighbor <- neighbors) yield
        Message(this, neighbor, mindist + 1.0)
    } else
      List()
  }
}

object ShortestPathVertex {

  def runSSSP(size: Int) {
    println("Creating new graph...")
    val g = new Graph[Double]
    val q = new Queue[Vertex[Double]]

    var root = g.addVertex(new ShortestPathVertex("source", true))
    for (i <- 1 until size) {
      val v = g.addVertex(new ShortestPathVertex(i.toString))
      q += v
      root.connectTo(v)
      if (i % 2 == 0) {
        root = q.dequeue()
      }
    }

    g.start()
    g.iterate(size)

    println("Values after computation:")
    g.synchronized {
      for (v <- g.vertices) {
        println(v.label + ": " + v.value)
      }
    }

    g.terminate()
  }

  def main(args: Array[String]) {
    runSSSP(args(0).toInt)
  }
}

package pagerank

import menthor.processing.{ SimpleVertex, Message }

class PageRankVertex(val label: String, _margin: Double = 0.00001d, val damping: Double = 0.85d, val iterations: Int = 0) extends SimpleVertex[Double] {
  val initialValue = 0.0d

  val margin = _margin.abs

  def update = process {
    val sum = incoming.map(_.value).sum
    val newvalue = ((1.0d - damping) / numVertices) + damping * sum
    val diff = (newvalue - value).abs
    value = newvalue

    if ((diff > margin) && (step != iterations)) {
      val n = neighbors.size
      for (neighbor <- neighbors)
        yield Message(neighbor, value / n)
    } else {
      voteToHalt
    }
  }
}

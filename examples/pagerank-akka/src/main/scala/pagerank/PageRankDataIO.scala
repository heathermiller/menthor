package pagerank

import menthor.io.{ DataIO, DataIOMaster, AbstractDataIO }
import menthor.processing.Vertex
import akka.actor.Uuid

import scala.io.Source
import java.io.{ File, FileOutputStream, PrintStream }

class PageRankDataIO(
  val workerUuid: Uuid,
  val workerVertices: Array[(Int,(String,List[Int]))]
)(
  implicit val opts: PageRankOptions,
  implicit val workers: IndexedSeq[Uuid],
  implicit var binaryWorker: Map[Uuid, Array[Byte]],
  implicit val numVertices: Int
) extends AbstractDataIO[Double,Int] {
  @transient var _vertices: Map[Int,(String,List[Int])] = null

  def vertices(worker: Uuid) = {
    if (worker == workerUuid) {
      if (_vertices eq null)
        _vertices = Map(workerVertices: _*)
      _vertices.mapValues(i => List(i._2: _*))
    } else
      Map.empty[Int,Iterable[Int]]
  }

  def ownerUuid(vid: Int) = workers(vid % workers.size)

  def createVertex(vid: Int): Vertex[Double] = {
    if (opts.iterations > 0)
      new PageRankVertex(_vertices(vid)._1, iterations = opts.iterations)
    else
      new PageRankVertex(_vertices(vid)._1)
  }

  def processVertices(worker: Uuid, vs: Iterable[Vertex[Double]]) {
    val resultsFile = new File(opts.resultsDir, worker.toString)
    val output = new PrintStream(new FileOutputStream(resultsFile))
    for (v <- vs) v match {
      case prv: PageRankVertex =>
        output.println(prv.label + " " + prv.value)
    }
  }
}

class PageRankDataIOMaster(val opts: PageRankOptions) extends DataIOMaster[Double] {
  implicit var workers = IndexedSeq.empty[Uuid]
  implicit var numVertices = 0
  implicit def propts = opts

  var vertices = Map.empty[Uuid, Array[(Int,(String,List[Int]))]]

  def workerIO(worker: Uuid): DataIO[Double] = new PageRankDataIO(worker, vertices(worker))

  def partitionVertices(topology: List[List[Uuid]]) {
    workers = topology.flatten.toIndexedSeq

    val titles = Source.fromFile(opts.titles).getLines.toIndexedSeq
    def title(vid: Int) = {
      if (vid < titles.size)
        titles(vid)
      else
        "no_title[" + vid + "]"
    }

    var links = Source.fromFile(opts.links).getLines.take(opts.pages).map { line =>
      val (vid, neighbors) = PageRankGraphParser.parseAll(PageRankGraphParser.vertex, line).get
      (vid -> (title(vid), neighbors))
    }.toMap

    val allVertices = links.flatMap(l => l._1 :: l._2._2).toSet
    numVertices = allVertices.size

    links ++= (for (v <- (allVertices -- links.keys)) yield (v -> (title(v), List.empty[Int])))

    for (i <- workers.indices)
      vertices += (workers(i) -> links.filterKeys(_ % workers.size == i).toArray)
  }
}

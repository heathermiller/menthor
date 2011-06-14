package pagerank

import menthor.io.{ DataIO, AbstractDataIO }
import menthor.processing.Vertex
import akka.actor.Uuid

class PageRankDataIO(val opts: PageRankOptions) extends AbstractDataIO[Double,Int] {
  def numVertices: Int = 0
  def vertices(worker: Uuid): Map[VertexID, Iterable[VertexID]] = Map.empty
  def ownerUuid(vid: VertexID): Uuid = null
  def createVertex(vid: VertexID): Vertex[Double] = null
  def processVertices(worker: Uuid, vertices: Iterable[Vertex[Double]]) { }
  def workerIO(worker: Uuid): DataIO[Double] = this
  def partitionVertices(topology: List[List[Uuid]]) { }
}

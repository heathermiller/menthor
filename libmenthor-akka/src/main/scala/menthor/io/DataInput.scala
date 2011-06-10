package menthor.io

import menthor.processing.{Vertex, VertexRef}

import akka.actor.ActorRef

trait DataInput[Data] extends Serializable {
  type VertexID
  implicit def vidmanifest: Manifest[VertexID]

  def numVertices: Int
  def vertices(worker: ActorRef): Map[VertexID, Iterable[VertexID]]
  def owner(vid: VertexID): ActorRef
  def createVertex(vid: VertexID): Vertex[Data]
}

abstract class AbstractDataInput[Data, VID: Manifest] extends DataInput[Data] {
  type VertexID = VID
  def vidmanifest = manifest[VID]
}

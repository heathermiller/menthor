package menthor.akka.processing

import akka.actor.ActorRef

trait DataInput[Data, VertexID] extends Serializable {
  def ownedVertices(worker: ActorRef): Map[VertexID, Iterable[VertexID]]
  def vertexOwner(vid: VertexID): ActorRef
  def createVertex(vid: VertexID): Vertex[Data]
}

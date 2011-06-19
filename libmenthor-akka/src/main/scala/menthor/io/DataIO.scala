package menthor.io

import menthor.processing.{Vertex, VertexRef}

import akka.actor.{ActorRef, Uuid}
import akka.actor.Actor.registry
import akka.serialization.RemoteActorSerialization._

import collection.mutable

trait DataIOMaster[Data] {
  protected implicit var binaryWorker = Map.empty[Uuid, Array[Byte]]

  protected def partitionVertices(topology: List[List[Uuid]]): Unit

  private[menthor] def useWorkers(workers: List[Map[Uuid, ActorRef]]) {
    partitionVertices(workers.map(_.keys.toList))
    for ((uuid, worker) <- workers.flatten)
      binaryWorker += uuid -> toRemoteActorRefProtocol(worker).toByteArray
  }

  def workerIO(worker: Uuid): DataIO[Data]
}

trait DataIO[Data] extends Serializable {
  type VertexID
  implicit def vidmanifest: Manifest[VertexID]

  def numVertices: Int
  def vertices(worker: Uuid): Map[VertexID, Iterable[VertexID]]
  def ownerUuid(vid: VertexID): Uuid
  def createVertex(vid: VertexID): Vertex[Data]
  def processVertices(worker: Uuid, vertices: Iterable[Vertex[Data]])

  protected var binaryWorker: Map[Uuid, Array[Byte]]
  @transient private var workerRef: mutable.Map[Uuid, ActorRef] = null

  private[menthor] def owner(vid: VertexID): ActorRef = {
    val uuid = ownerUuid(vid)
    if (workerRef eq null)
      workerRef = mutable.Map.empty
    workerRef.getOrElseUpdate(uuid, workerActorRef(uuid))
  }

  private[menthor] def workerActorRef(uuid: Uuid): ActorRef = {
    registry.actorFor(uuid).getOrElse {
      fromBinaryToRemoteActorRef(binaryWorker(uuid))
    }
  }
}

abstract class AbstractDataIO[Data, VID: Manifest] extends DataIO[Data] {
  type VertexID = VID
  def vidmanifest = manifest[VID]
}

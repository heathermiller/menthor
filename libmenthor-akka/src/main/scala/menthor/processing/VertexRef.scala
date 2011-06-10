package menthor.processing

import akka.actor.{Actor, ActorRef, newUuid, Uuid}
import akka.util.HashCode

class VertexRef(_uuid: Option[Uuid] = None, _worker: Option[ActorRef] = None)
extends Comparable[VertexRef] with Equals with Serializable {
  val uuid = _uuid getOrElse newUuid
  val worker = _worker getOrElse {
    Worker.vertexCreator.get match {
      case Some(wrkr) => wrkr.self
      case None => throw new IllegalStateException("Not inside a worker")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: VertexRef => (that canEqual this) && (uuid == this.uuid)
    case _ => false
  }

  def canEqual(other: Any): Boolean =
    other.isInstanceOf[VertexRef]

  override def hashCode: Int = HashCode.hash(HashCode.SEED, uuid)

  override def toString = "Vertex[" + uuid + "]"

  def compareTo(other: VertexRef) = this.uuid compareTo other.uuid
}

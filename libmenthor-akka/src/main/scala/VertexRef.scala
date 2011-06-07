package menthor.akka.processing

import akka.actor.{Actor, ActorRef, newUuid}

class VertexRef(workerRef: ActorRef) extends Comparable[VertexRef] {
  val uuid = newUuid
  val worker = Actor.registry.actorFor(workerRef.uuid) getOrElse workerRef

  def compareTo(other: VertexRef) = this.uuid compareTo other.uuid
}

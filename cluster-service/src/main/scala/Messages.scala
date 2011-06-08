package menthor.akka.cluster

import akka.actor.ActorRef
import akka.actor.Actor.registry
import akka.serialization.RemoteActorSerialization._

sealed abstract class ClusterServiceMessage

case object AvailableProcessors extends ClusterServiceMessage
case class AvailableProcessors(count: Int) extends ClusterServiceMessage

class RemoteActorRefMessage(actor: ActorRef) extends ClusterServiceMessage with Serializable {
  val bin = toRemoteActorRefProtocol(actor).toByteArray
}

object CreateForeman {
  def apply[D: Manifest](parent: ActorRef) = new CreateForeman[D](parent)

  def unapply(msg: CreateForeman[_]): Option[ActorRef] = {
    if (msg eq null) None
    else {
      val parentRef = fromBinaryToRemoteActorRef(msg.bin)
      registry.actorFor(parentRef.uuid) orElse Some(parentRef)
    }
  }
}

class CreateForeman[D](parent: ActorRef)(implicit val manifest: Manifest[D]) extends RemoteActorRefMessage(parent) {
  type Data = D
}

object ForemanCreated {
  def apply(foreman: ActorRef) = new ForemanCreated(foreman)

  def unapply(msg: ForemanCreated): Option[ActorRef] = {
    if (msg eq null) None
    else {
      val foremanRef = fromBinaryToRemoteActorRef(msg.bin)
      registry.actorFor(foremanRef.uuid) orElse Some(foremanRef)
    }
  }
}

class ForemanCreated(foreman: ActorRef) extends RemoteActorRefMessage(foreman)

case class CreateWorkers(count: Int) extends ClusterServiceMessage

object WorkersCreated {
  def apply(workers: List[ActorRef]) = new WorkersCreated(workers)

  def unapply(msg: WorkersCreated): Option[List[ActorRef]] = {
    if (msg eq null) None
    else {
      Some(msg.bins map { workerData =>
        val workerRef = fromBinaryToRemoteActorRef(workerData)
        registry.actorFor(workerRef.uuid) getOrElse workerRef
      } )
    }
  }
}

class WorkersCreated(workers: List[ActorRef]) extends ClusterServiceMessage with Serializable {
  val bins = workers.map(toRemoteActorRefProtocol(_).toByteArray)
}

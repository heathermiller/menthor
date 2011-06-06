package menthor.akka.cluster

import akka.actor.ActorRef
import akka.actor.Actor.registry
import akka.serialization.RemoteActorSerialization._

sealed abstract class ClusterServiceMessage

class RemoteActorRefMessage(actor: ActorRef) extends ClusterServiceMessage with Serializable {
  val bin = toRemoteActorRefProtocol(actor).toByteArray
}

object CreateForeman {
  def apply(parent: ActorRef) = new CreateForeman(parent)

  def unapply(data: Any) : Option[ActorRef] = {
    if (! data.isInstanceOf[CreateForeman]) None
    else {
      val parentRef = fromBinaryToRemoteActorRef(data.asInstanceOf[CreateForeman].bin)
      registry.actorFor(parentRef.uuid) orElse Some(parentRef)
    }
  }
}

class CreateForeman(parent: ActorRef) extends RemoteActorRefMessage(parent)

object ForemanCreated {
  def apply(foreman: ActorRef) = new ForemanCreated(foreman)

  def unapply(data: Any) : Option[ActorRef] = {
    if (! data.isInstanceOf[ForemanCreated]) None
    else {
      val foremanRef = fromBinaryToRemoteActorRef(data.asInstanceOf[ForemanCreated].bin)
      registry.actorFor(foremanRef.uuid) orElse Some(foremanRef)
    }
  }
}

class ForemanCreated(foreman: ActorRef) extends RemoteActorRefMessage(foreman)

case class CreateWorkers(count: Int) extends ClusterServiceMessage

object WorkersCreated {
  def apply(workers: List[ActorRef]) = new WorkersCreated(workers)

  def unapply(data: Any) : Option[List[ActorRef]] = {
    if (! data.isInstanceOf[WorkersCreated]) None
    else {
      Some(data.asInstanceOf[WorkersCreated].bins map { workerData =>
        val workerRef = fromBinaryToRemoteActorRef(workerData)
        registry.actorFor(workerRef.uuid) getOrElse workerRef
      } )
    }
  }
}

class WorkersCreated(workers: List[ActorRef]) extends ClusterServiceMessage with Serializable {
  val bins = workers.map(toRemoteActorRefProtocol(_).toByteArray)
}

case object AvailableProcessors extends ClusterServiceMessage
case class AvailableProcessors(count: Int) extends ClusterServiceMessage

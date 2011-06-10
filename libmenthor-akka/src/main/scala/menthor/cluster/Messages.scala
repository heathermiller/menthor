package menthor.cluster

import akka.actor.{ActorRef, LocalActorRef, Uuid}
import akka.actor.Actor.registry
import akka.serialization.RemoteActorSerialization._

sealed abstract class ClusterServiceMessage extends Serializable

case object AvailableProcessors extends ClusterServiceMessage
case class AvailableProcessors(count: Int) extends ClusterServiceMessage

abstract class RemoteActorRefMessage(actor: ActorRef) extends ClusterServiceMessage {
  val bin = toRemoteActorRefProtocol(actor).toByteArray
  val uuid = {
    if (actor.isInstanceOf[LocalActorRef]) Some(actor.uuid)
    else None
  }
}

trait RemoteActorRefExtractor[Message <: RemoteActorRefMessage] {
  def unapply(msg: Message): Option[ActorRef] = {
    if (msg eq null) None
    else msg.uuid match {
      case Some(uuid) => registry.actorFor(uuid) orElse Some(fromBinaryToRemoteActorRef(msg.bin))
      case None => Some(fromBinaryToRemoteActorRef(msg.bin))
    }
  }
}

abstract class RemoteActorRefSeqMessage(val msgs: Iterable[RemoteActorRefMessage]) extends ClusterServiceMessage

trait RemoteActorRefSeqExtractor[Message <: RemoteActorRefSeqMessage] {
  def unapply(msg: Message): Option[Iterable[ActorRef]] = {
    if (msg eq null) None
    else Some {
      for (message <- msg.msgs) yield {
        message.uuid match {
          case Some(uuid) => registry.actorFor(uuid) getOrElse fromBinaryToRemoteActorRef(message.bin)
          case None => fromBinaryToRemoteActorRef(message.bin)
        }
      }
    }
  }
}

object CreateForeman extends RemoteActorRefExtractor[CreateForeman[_]] {
  def apply[D: Manifest](parent: ActorRef) = new CreateForeman[D](parent)
}

class CreateForeman[D](parent: ActorRef)(implicit val manifest: Manifest[D]) extends RemoteActorRefMessage(parent) {
  type Data = D
}

object ForemanCreated extends RemoteActorRefExtractor[ForemanCreated] {
  def apply(foreman: ActorRef) = new ForemanCreated(foreman)
}

class ForemanCreated(foreman: ActorRef) extends RemoteActorRefMessage(foreman)

case class CreateWorkers(count: Int) extends ClusterServiceMessage

object WorkersCreated extends RemoteActorRefSeqExtractor[WorkersCreated] {
  def apply(workers: Iterable[ActorRef]) = {
    val msgs =
      for (worker <- workers) yield
        new WorkerCreated(worker)
    new WorkersCreated(msgs)
  }
}

class WorkerCreated(worker: ActorRef) extends RemoteActorRefMessage(worker)

class WorkersCreated(msgs: Iterable[WorkerCreated]) extends RemoteActorRefSeqMessage(msgs)

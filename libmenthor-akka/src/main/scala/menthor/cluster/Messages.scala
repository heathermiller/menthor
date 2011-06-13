package menthor.cluster

import akka.actor.{ActorRef, LocalActorRef, Uuid}
import akka.actor.Actor.registry
import akka.serialization.RemoteActorSerialization._

sealed abstract class ClusterServiceMessage extends Serializable

case object AvailableProcessors extends ClusterServiceMessage
case class AvailableProcessors(count: Int) extends ClusterServiceMessage

class RemoteActorRefData(val bin: Array[Byte], val uuid: Option[Uuid]) extends Serializable {
  def this(actorref: ActorRef, uuid: Option[Uuid] = None) =
    this(
      toRemoteActorRefProtocol(actorref).toByteArray,
      uuid orElse { actorref match {
        case _: LocalActorRef => Some(actorref.uuid)
        case _ => None
      } } )
  def this(actorref: ActorRef, uuid: Uuid) = this(actorref, Some(uuid))

  def actor: ActorRef = uuid match {
    case Some(u) => registry.actorFor(u) getOrElse fromBinaryToRemoteActorRef(bin)
    case None => fromBinaryToRemoteActorRef(bin)
  }
}

object CreateForeman {
  def apply[D: Manifest](parent: ActorRef) = new CreateForeman[D](parent)
  def unapply(msg: CreateForeman[_]): Option[ActorRef] = Some(msg.ref.actor)
}

class CreateForeman[D](parent: ActorRef)(implicit val manifest: Manifest[D]) extends ClusterServiceMessage {
  type Data = D
  val ref = new RemoteActorRefData(parent)
}

object ForemanCreated {
  def apply(foreman: ActorRef, supervisor: ActorRef) = new ForemanCreated(foreman, supervisor)
  def unapply(msg: ForemanCreated): Option[(ActorRef, (Uuid, ActorRef))] = Some((msg.fref.actor, (msg.sref.uuid.get, msg.sref.actor)))
}

class ForemanCreated(foreman: ActorRef, supervisor: ActorRef) extends ClusterServiceMessage {
  val fref = new RemoteActorRefData(foreman)
  val sref = new RemoteActorRefData(supervisor)
}

case class CreateWorkers(count: Int) extends ClusterServiceMessage

object WorkersCreated {
  def apply(workers: Iterable[ActorRef]) = new WorkersCreated(workers)
  def unapply(msg: WorkersCreated): Option[Map[Uuid, ActorRef]] = Some(msg.wrefs.mapValues(_.actor))
}

class WorkersCreated(workers: Iterable[ActorRef]) extends ClusterServiceMessage {
  val wrefs: Map[Uuid, RemoteActorRefData] = workers.map(w => (w.uuid -> new RemoteActorRefData(w))).toMap
}

object GraphSupervisors {
  def apply(supervisors: Map[Uuid, ActorRef]) = new GraphSupervisors(supervisors)
  def unapply(msg: GraphSupervisors): Option[Iterable[ActorRef]] = Some(msg.srefs.map(_.actor))
}

class GraphSupervisors(supervisors: Map[Uuid, ActorRef]) extends ClusterServiceMessage {
  val srefs: List[RemoteActorRefData] = supervisors.map(pair => new RemoteActorRefData(pair._2, pair._1)).toList
}

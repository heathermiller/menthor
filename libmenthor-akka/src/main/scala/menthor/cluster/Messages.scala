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
  def apply(foreman: ActorRef, supervisor: ActorRef, monitor: ActorRef) = new ForemanCreated(foreman, supervisor, monitor)
  def unapply(msg: ForemanCreated): Option[(ActorRef, (ActorRef, Uuid, ActorRef))] = Some((msg.fref.actor, (msg.sref.actor, msg.mref.uuid.get, msg.mref.actor)))
}

class ForemanCreated(foreman: ActorRef, supervisor: ActorRef, monitor: ActorRef) extends ClusterServiceMessage {
  val fref = new RemoteActorRefData(foreman)
  val sref = new RemoteActorRefData(supervisor)
  val mref = new RemoteActorRefData(monitor)
}

case class CreateWorkers(count: Int) extends ClusterServiceMessage

object WorkersCreated {
  def apply(workers: Iterable[ActorRef]) = new WorkersCreated(workers)
  def unapply(msg: WorkersCreated): Option[Map[Uuid, ActorRef]] = Some(Map(msg.wrefs: _*).mapValues(_.actor))
}

class WorkersCreated(workers: Iterable[ActorRef]) extends ClusterServiceMessage {
  val wrefs: Array[(Uuid, RemoteActorRefData)] = workers.map(w => (w.uuid -> new RemoteActorRefData(w))).toArray
}

object GraphCFMs {
  def apply(cfms: Map[Uuid, ActorRef]) = new GraphCFMs(cfms)
  def unapply(msg: GraphCFMs): Option[Set[ActorRef]] = Some(List(msg.srefs: _*).map(_.actor).toSet)
}

class GraphCFMs(cfms: Map[Uuid, ActorRef]) extends ClusterServiceMessage {
  val srefs: Array[RemoteActorRefData] = cfms.map(pair => new RemoteActorRefData(pair._2, pair._1)).toArray
}

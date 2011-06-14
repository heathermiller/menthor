package menthor.cluster

import akka.AkkaException
import akka.actor.{ Actor, ActorRef }
import akka.actor.MaximumNumberOfRestartsWithinTimeRangeReached
import akka.config.Supervision.{ AllForOneStrategy, Permanent }

class NodeFailure(cause: Throwable) extends AkkaException(cause = cause)

class ClusterFailureMonitor extends Actor {
  self.lifeCycle = Permanent
  def receive = { case e: NodeFailure => throw e }
}

class GraphSupervisor(var _someCfm: Option[ActorRef]) extends Actor {
  def this() = this(None)
  def this(cfm: ActorRef) = this(Some(cfm))

  def cfm = _someCfm.get

  self.faultHandler = AllForOneStrategy(List(classOf[Throwable]), 0, -1)

  private var monitors = Set.empty[ActorRef]

  override def receive = {
    case GraphCFMs(cfms) =>
      if (_someCfm.isDefined) {
        self.link(cfm)
        monitors = cfms - cfm
      } else monitors = cfms
    case MaximumNumberOfRestartsWithinTimeRangeReached(victim, _, _, cause)
    if (_someCfm.isDefined && victim == cfm) =>
      cause match {
        case _: NodeFailure =>
          self.stop()
        case _ =>
          for (cfm <- monitors)
            cfm ! new NodeFailure(cause)
          self.stop()
      }
  }

  override def postStop() {
    if (_someCfm.isDefined)
      cfm.stop()
    val i = self.linkedActors.values.iterator
    while (i.hasNext)
      self.unlink(i.next)
  }
}

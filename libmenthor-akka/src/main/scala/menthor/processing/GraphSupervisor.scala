package menthor.cluster

import akka.actor.{Actor, ActorRef, Exit}
import akka.config.Supervision.{AllForOneStrategy, Permanent}

import menthor.MenthorException

class GraphSupervisorException private[menthor](message: String, cause: Throwable = null) extends MenthorException(message, cause)

class GraphSupervisor extends Actor {
  self.faultHandler = AllForOneStrategy(List(classOf[Throwable]), None, None)

  var graphSupervisors: List[ActorRef] = Nil

  val lifeCyle = Permanent

  def receive = {
    case GraphSupervisors(supervisors) =>
      for {
        supervisor <- supervisors
        if (supervisor != self)
      } graphSupervisors = supervisor :: graphSupervisors
  }

  override def preRestart(cause: Throwable) {
    val e = new GraphSupervisorException(
      "", cause
    )
    for (supervisor <- graphSupervisors)
      supervisor ! Exit(null, e)
  }

  override def postRestart(cause: Throwable) {
    self.stop()
  }

  override def postStop() {
    val i = self.linkedActors.values.iterator
    while (i.hasNext)
      self.unlink(i.next)
  }
}

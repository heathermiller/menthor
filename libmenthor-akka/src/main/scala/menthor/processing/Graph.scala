package menthor.processing

import Crunch.reduceCrunch
import WorkerStatusMessage.reduceStatusMessage

import akka.actor.{Actor, ActorRef, Uuid, UnlinkAndStop}
import akka.event.EventHandler

class Graph[Data](val childrenCount: Int) extends Actor {
  var children = Set.empty[ActorRef]

  override def postStop() {
    if (self.supervisor.isDefined)
      self.supervisor.get ! UnlinkAndStop(self)
  }

  def receive = {
    case SetupDone =>
      assert(self.sender.isDefined)
      children += self.sender.get
      if (childrenCount == children.size) {
        become(processing)
        for (child <- children)
          child ! Next
      }
  }

  def processing: Actor.Receive = {
    case msg: WorkerStatusMessage =>
      assert(self.sender.isDefined)

      if (childrenCount > 1) become(stepStatus(childrenCount - 1, msg), false)
      else msg match {
        case Halt =>
          EventHandler.debug(this, "Computation complete")
          for (child <- children)
            child ! Stop
          self.stop()
        case Done =>
          EventHandler.debug(this, "Starting next superstep")
          for (child <- children)
            child ! Next
      }
    case _crunch: Crunch[_] =>
      assert(self.sender.isDefined)
      val crunch = _crunch.asInstanceOf[Crunch[Data]]

      if (childrenCount > 1) become(crunching(childrenCount - 1, crunch), false)
      else {
        for (child <- children)
          child ! CrunchResult(crunch.result)
      }
  }

  def stepStatus(remaining: Int, status: WorkerStatusMessage): Actor.Receive = {
    case msg: WorkerStatusMessage =>
      assert(self.sender.isDefined && remaining > 0)
      val _status = reduceStatusMessage(status, msg)

      if (remaining > 1) become(stepStatus(remaining - 1, _status))
      else _status match {
        case Halt =>
          EventHandler.debug(this, "Computation complete")
          for (child <- children)
            child ! Stop
          self.stop()
        case Done =>
          EventHandler.debug(this, "Starting next superstep")
          unbecome()
          for (child <- children)
            child ! Next
      }
    case _: Crunch[_] =>
      throw new InvalidStepException("Mixing crunches and substeps in the same step")
  }

  def crunching(remaining: Int, crunch1: Crunch[Data]): Actor.Receive = {
    case _crunch2: Crunch[_] =>
      assert(self.sender.isDefined && remaining > 0)
      val crunch2 = _crunch2.asInstanceOf[Crunch[Data]]
      val crunch = reduceCrunch(crunch1, crunch2)

      if (remaining > 1) become(crunching(remaining - 1, crunch))
      else {
        EventHandler.debug(this, "Crunch complete")
        unbecome()
        for (child <- children)
          child ! CrunchResult(crunch.result)
      }
    case _: WorkerStatusMessage =>
      throw new InvalidStepException("Mixing crunches and substeps in the same step")
  }
}

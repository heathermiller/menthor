package menthor.processing

import Crunch.reduceCrunch
import WorkerStatusMessage.reduceStatusMessage

import akka.actor.{Actor, ActorRef, Uuid}

class Graph[Data](val childrenCount: Int) extends Actor {
  def receive = setup(Set.empty)

  def setup(children: Set[ActorRef]): Actor.Receive = {
    case SetupDone =>
      assert(self.sender.isDefined)
      val _children = children + self.sender.get
      if (childrenCount == children.size)
        become(processing(children))
      else become(setup(_children))
  }

  def processing(children: Set[ActorRef]): Actor.Receive = {
    case msg: WorkerStatusMessage =>
      assert(self.sender.isDefined)

      if (childrenCount > 1) become(stepStatus(children, childrenCount - 1, msg), false)
      else msg match {
        case Halt =>
          for (child <- children)
            child ! Stop
          self.stop()
        case Done =>
          for (child <- children)
            child ! Next
      }
    case _crunch: Crunch[_] =>
      assert(self.sender.isDefined)
      val crunch = _crunch.asInstanceOf[Crunch[Data]]

      if (childrenCount > 1) become(crunching(children, childrenCount - 1, crunch), false)
      else {
        for (child <- children)
          child ! CrunchResult(crunch.result)
      }
  }

  def stepStatus(children: Set[ActorRef], remaining: Int, status: WorkerStatusMessage): Actor.Receive = {
    case msg: WorkerStatusMessage =>
      assert(self.sender.isDefined && remaining > 0)
      val _status = reduceStatusMessage(status, msg)

      if (remaining > 1) become(stepStatus(children, remaining - 1, _status))
      else _status match {
        case Halt =>
          for (child <- children)
            child ! Stop
          self.stop()
        case Done =>
          unbecome()
          for (child <- children)
            child ! Next
      }
  }

  def crunching(children: Set[ActorRef], remaining: Int, crunch1: Crunch[Data]): Actor.Receive = {
    case _crunch2: Crunch[_] =>
      assert(self.sender.isDefined && remaining > 0)
      val crunch2 = _crunch2.asInstanceOf[Crunch[Data]]
      val crunch = reduceCrunch(crunch1, crunch2)

      if (remaining > 1) become(crunching(children, remaining - 1, crunch))
      else {
        unbecome()
        for (child <- children)
          child ! CrunchResult(crunch.result)
      }
  }
}

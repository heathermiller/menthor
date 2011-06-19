package menthor.processing

import Crunch.reduceCrunch
import WorkerStatusMessage.reduceStatusMessage

import akka.actor.{Actor, ActorRef, Uuid}

class Graph[Data](val childrenCount: Int) extends Actor {
  var children = Map.empty[ActorRef, Set[Uuid]]

  override def postStop() {
    if (self.supervisor.isDefined)
      self.supervisor.get.stop()
  }

  def receive = {
    case SetupDone(workers) =>
      assert(self.sender.isDefined)
      children += (self.sender.get -> workers)
      if (childrenCount == children.size) {
        become(processing(childrenCount))
        for ((child, _) <- children)
          child ! Next(Map.empty)
      }
  }

  def processing(remaining: Int): Actor.Receive = {
    case msg: WorkerMessage =>
      assert(self.sender.isDefined)
      msg match {
        case NothingToDo =>
          if (remaining > 1) become(processing(remaining - 1))
          else
            for (child <- children.keys)
              child ! Stop(Map.empty)
        case wsm: WorkerStatusMessage =>
          if (remaining > 1) become(stepStatus(remaining - 1, wsm))
          else msg match {
            case Halt(info) =>
              for ((child, uuids) <- children)
                child ! Stop(info.filterKeys(uuids contains _))
              self.stop()
            case Done(info) =>
              for ((child, uuids) <- children)
                child ! Next(info.filterKeys(uuids contains _))
          }
        case _crunch: Crunch[_] =>
          val crunch = _crunch.asInstanceOf[Crunch[Data]]
          if (childrenCount > 1) become(crunching(remaining - 1, crunch))
          else
            for (child <- children.keys)
              child ! CrunchResult(crunch.result)
      }
  }

  def stepStatus(remaining: Int, status: WorkerStatusMessage): Actor.Receive = {
    case msg: WorkerStatusMessage =>
      assert(self.sender.isDefined && remaining > 0)
      val _status = reduceStatusMessage(status, msg)

      if (remaining > 1) become(stepStatus(remaining - 1, _status))
      else _status match {
        case Halt(info) =>
          for ((child, uuids) <- children)
            child ! Stop(info.filterKeys(uuids contains _))
          self.stop()
        case Done(info) =>
          become(processing(childrenCount))
          for ((child, uuids) <- children)
            child ! Next(info.filterKeys(uuids contains _))
      }
    case _: Crunch[_] =>
      throw new InvalidStepException("Mixing crunches and substeps in the same step")
  }

  def crunching(remaining: Int, crunch1: Crunch[Data]): Actor.Receive = {
    case crunch2: CrunchMessage =>
      assert(self.sender.isDefined && remaining > 0)
      val crunch = reduceCrunch(crunch1, crunch2)

      if (remaining > 1) become(crunching(remaining - 1, crunch))
      else {
        become(processing(childrenCount))
        for (child <- children.keys)
          child ! CrunchResult(crunch.result)
      }
    case _: WorkerStatusMessage =>
      throw new InvalidStepException("Mixing crunches and substeps in the same step")
  }
}

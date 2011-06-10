package menthor.cluster

import menthor.processing.Worker
import menthor.processing.{ Stop, Next, WorkerStatusMessage, Done, Halt, Crunch, CrunchResult, SetupDone }
import Crunch.reduceCrunch
import WorkerStatusMessage.reduceStatusMessage

import akka.actor.{Actor, ActorRef}
import collection.mutable.ListBuffer

class Foreman[Data: Manifest](val parent: ActorRef) extends Actor {
  def receive = {
    case CreateWorkers(count) =>
      if (count == 0) {
        become(fromGraph(Set.empty))
        parent ! SetupDone
      } else {
        var children = Set.empty[ActorRef]
        for (i <- 1 to count)
          children += Actor.actorOf(new Worker[Data](self)).start()
        become(setup(children, children))
        self.channel ! WorkersCreated(children)
      }
  }

  def setup(children: Set[ActorRef], remaining: Set[ActorRef]): Actor.Receive = {
    case SetupDone =>
      assert(self.sender.isDefined && remaining.nonEmpty)
      val worker = self.sender.get
      assert(remaining contains worker)

      if ((remaining - worker) isEmpty) {
        become(fromGraph(children))
        parent ! SetupDone
      } else become(setup(children, remaining - worker))
  }

  def fromGraph(children: Set[ActorRef]): Actor.Receive = {
    case Stop =>
      for (child <- children)
        child ! Stop
      self.stop()
    case msg @ (Next | _ : CrunchResult[_]) =>
      for (child <- children)
        child ! msg
      if (children.nonEmpty)
        become(processing(children), false)
    }

  def processing(children: Set[ActorRef]): Actor.Receive = {
    case msg: WorkerStatusMessage =>
      assert(self.sender.isDefined)
      val worker = self.sender.get
      assert(children contains worker)

      if ((children - worker) isEmpty) {
        unbecome()
        parent ! msg
      } else become(stepStatus(children, children - worker, msg))
    case _crunch: Crunch[_] =>
      assert(self.sender.isDefined)
      val worker = self.sender.get
      assert(children contains worker)
      val crunch = _crunch.asInstanceOf[Crunch[Data]]

      if ((children - worker) isEmpty) {
        unbecome()
        parent ! crunch
      } else become(crunching(children, children - worker, crunch))
  }

  def stepStatus(children: Set[ActorRef], remaining: Set[ActorRef], status: WorkerStatusMessage): Actor.Receive = {
    case msg: WorkerStatusMessage =>
      assert(self.sender.isDefined && remaining.nonEmpty)
      val worker = self.sender.get
      assert(remaining contains worker)
      val _status = reduceStatusMessage(status, msg)

      if ((remaining - worker) isEmpty) {
        unbecome()
        parent ! _status
      } else become(stepStatus(children, remaining - worker, _status))
  }

  def crunching(children: Set[ActorRef], remaining: Set[ActorRef], crunch1: Crunch[Data]): Actor.Receive = {
    case _crunch2: Crunch[_] =>
      assert(self.sender.isDefined && remaining.nonEmpty)
      val worker = self.sender.get
      assert(remaining contains worker)
      val crunch2 = _crunch2.asInstanceOf[Crunch[Data]]
      val crunch = reduceCrunch(crunch1, crunch2)

      if ((remaining - worker) isEmpty) {
        unbecome()
        parent ! crunch
      } else become(crunching(children, remaining - worker, crunch))
  }
}

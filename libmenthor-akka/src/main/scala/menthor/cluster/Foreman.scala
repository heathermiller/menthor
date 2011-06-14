package menthor.cluster

import menthor.processing.{ Worker, InvalidStepException }
import menthor.processing.{ Stop, Next, WorkerStatusMessage, Done, Halt, Crunch, CrunchResult, SetupDone }
import Crunch.reduceCrunch
import WorkerStatusMessage.reduceStatusMessage

import akka.actor.{Actor, ActorRef}
import collection.mutable.ListBuffer

class Foreman[Data: Manifest](val parent: ActorRef) extends Actor {
  var children = Set.empty[ActorRef]

  override def postStop() {
    if (self.supervisor.isDefined)
      self.supervisor.get.stop()
  }

  def receive = {
    case CreateWorkers(count) =>
      assert(count != 0)
      children = List.fill(count)(Actor.actorOf(new Worker[Data](self)).start()).toSet
      if (self.supervisor.isDefined) {
        val supervisor = self.supervisor.get
        for (child <- children)
          supervisor.link(child)
      }
      become(setup(children))
      self.channel ! WorkersCreated(children)
  }

  def setup(remaining: Set[ActorRef]): Actor.Receive = {
    case SetupDone(_) =>
      assert(self.sender.isDefined && remaining.nonEmpty)
      val worker = self.sender.get
      assert(remaining contains worker)

      if ((remaining - worker) isEmpty) {
        become(fromGraph)
        parent ! SetupDone(children.map(_.uuid))
      } else become(setup(remaining - worker))
  }

  def fromGraph: Actor.Receive = {
    case Stop(info) =>
      for (child <- children)
        child ! Stop(info.filterKeys(_ == child.uuid))
      self.stop()
    case Next(info) =>
      for (child <- children)
        child ! Next(info.filterKeys(_ == child.uuid))
      become(processing, false)
    case msg @ CrunchResult(_) =>
      for (child <- children)
        child ! msg
      become(processing, false)
    }

  def processing: Actor.Receive = {
    case msg: WorkerStatusMessage =>
      assert(self.sender.isDefined)
      val worker = self.sender.get
      assert(children contains worker)

      if ((children - worker) isEmpty) {
        unbecome()
        parent ! msg
      } else become(stepStatus(children - worker, msg))
    case _crunch: Crunch[_] =>
      assert(self.sender.isDefined)
      val worker = self.sender.get
      assert(children contains worker)
      val crunch = _crunch.asInstanceOf[Crunch[Data]]

      if ((children - worker) isEmpty) {
        unbecome()
        parent ! crunch
      } else become(crunching(children - worker, crunch))
  }

  def stepStatus(remaining: Set[ActorRef], status: WorkerStatusMessage): Actor.Receive = {
    case msg: WorkerStatusMessage =>
      assert(self.sender.isDefined && remaining.nonEmpty)
      val worker = self.sender.get
      assert(remaining contains worker)
      val _status = reduceStatusMessage(status, msg)

      if ((remaining - worker) isEmpty) {
        unbecome()
        parent ! _status
      } else become(stepStatus(remaining - worker, _status))
    case _: Crunch[_] =>
      throw new InvalidStepException("Mixing crunches and substeps in the same step")
  }

  def crunching(remaining: Set[ActorRef], crunch1: Crunch[Data]): Actor.Receive = {
    case _crunch2: Crunch[_] =>
      assert(self.sender.isDefined && remaining.nonEmpty)
      val worker = self.sender.get
      assert(remaining contains worker)
      val crunch2 = _crunch2.asInstanceOf[Crunch[Data]]
      val crunch = reduceCrunch(crunch1, crunch2)

      if ((remaining - worker) isEmpty) {
        unbecome()
        parent ! crunch
      } else become(crunching(remaining - worker, crunch))
    case _: WorkerStatusMessage =>
      throw new InvalidStepException("Mixing crunches and substeps in the same step")
  }
}

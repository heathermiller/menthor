package menthor.cluster

import menthor.processing._
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
      become(processing(children))
    case msg @ CrunchResult(_) =>
      for (child <- children)
        child ! msg
      become(processing(children))
    }

  def processing(remaining: Set[ActorRef]): Actor.Receive = {
    case msg: WorkerMessage =>
      assert(self.sender.isDefined)
      val worker = self.sender.get
      assert(remaining contains worker)
      val _remaining = remaining - worker

      if ((_remaining) isEmpty) {
        become(fromGraph)
        parent ! msg
      } else msg match {
        case NothingToDo =>
          become(processing(_remaining))
        case wsm: WorkerStatusMessage =>
          become(stepStatus(_remaining, wsm))
        case _crunch: Crunch[_] =>
          val crunch = _crunch.asInstanceOf[Crunch[Data]]
          become(crunching(_remaining, crunch))
      }
  }

  def stepStatus(remaining: Set[ActorRef], status: WorkerStatusMessage): Actor.Receive = {
    case msg: WorkerStatusMessage =>
      assert(self.sender.isDefined && remaining.nonEmpty)
      val worker = self.sender.get
      assert(remaining contains worker)
      val _status = reduceStatusMessage(status, msg)

      if ((remaining - worker) isEmpty) {
        become(fromGraph)
        parent ! _status
      } else become(stepStatus(remaining - worker, _status))
    case _: Crunch[_] =>
      throw new InvalidStepException("Mixing crunches and substeps in the same step")
  }

  def crunching(remaining: Set[ActorRef], crunch1: Crunch[Data]): Actor.Receive = {
    case crunch2: CrunchMessage =>
      assert(self.sender.isDefined && remaining.nonEmpty)
      val worker = self.sender.get
      assert(remaining contains worker)
      val crunch = reduceCrunch(crunch1, crunch2)

      if ((remaining - worker) isEmpty) {
        become(fromGraph)
        parent ! crunch
      } else become(crunching(remaining - worker, crunch))
    case _: WorkerStatusMessage =>
      throw new InvalidStepException("Mixing crunches and substeps in the same step")
  }
}

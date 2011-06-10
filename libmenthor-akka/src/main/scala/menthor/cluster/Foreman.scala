package menthor.cluster

import menthor.processing.Worker
import menthor.processing.{ Stop, Next, WorkerStatusMessage, StopRequest, Done, Halt, Crunch, CrunchResult, SetupDone }
import Crunch.reduceCrunch
import WorkerStatusMessage.reduceStatusMessage

import akka.actor.{Actor, ActorRef}
import collection.mutable.ListBuffer

class Foreman[Data: Manifest](val parent: ActorRef) extends Actor {
  def receive = {
    case CreateWorkers(count) =>
      var children = Set.empty[ActorRef]
      for (i <- 1 to count)
        children += Actor.actorOf(new Worker[Data](self)).start()
      become(setup(children, children))
      self.channel ! WorkersCreated(children)
  }

  def setup(children: Set[ActorRef], remaining: Set[ActorRef]): Actor.Receive = {
    case SetupDone =>
      if (self.sender.isDefined) {
        val worker = self.sender.get
        if ((remaining - worker) isEmpty) {
          become(processing(children, children, Halt))
          parent ! SetupDone
        } else become(setup(children, remaining - worker))
      }
  }

  def processing(children: Set[ActorRef], remaining: Set[ActorRef], status: WorkerStatusMessage): Actor.Receive = {
    case Stop =>
      assert(remaining.isEmpty)
      for (child <- children)
        child ! Stop
      self.stop()
    case msg @ (Next | _ : CrunchResult[_]) =>
      assert(remaining.isEmpty)
      for (child <- children)
        child ! msg
      become(processing(children, children, Halt))
    case msg: WorkerStatusMessage =>
      assert(self.sender.isDefined && remaining.nonEmpty)
      val _status = reduceStatusMessage(msg, status)
      val worker = self.sender.get
      assert(remaining contains worker)

      if ((remaining - worker) isEmpty)
        parent ! _status
      become(processing(children, remaining - worker, _status))
    case _crunch: Crunch[_] =>
      assert(self.sender.isDefined && remaining.nonEmpty)
      assert(children.size == remaining.size)
      val worker = self.sender.get
      assert(remaining contains worker)
      val crunch = _crunch.asInstanceOf[Crunch[Data]]

      if ((remaining - worker) isEmpty) {
        parent ! crunch
        become(processing(children, remaining - worker, Halt))
      } else
        become(crunching(children, remaining - worker, crunch))
  }

  def crunching(children: Set[ActorRef], remaining: Set[ActorRef], crunch1: Crunch[Data]): Actor.Receive = {
    case _crunch2: Crunch[_] =>
      assert(self.sender.isDefined && remaining.nonEmpty)
      val worker = self.sender.get
      assert(remaining contains worker)
      val crunch2 = _crunch2.asInstanceOf[Crunch[Data]]
      val crunch = reduceCrunch(crunch1, crunch2)

      if ((remaining - worker) isEmpty) {
        parent ! crunch
        become(processing(children, remaining - worker, Halt))
      } else
        become(crunching(children, remaining - worker, crunch))
  }
}

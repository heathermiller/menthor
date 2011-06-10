package menthor.cluster

import menthor.processing.{Worker, SetupDone}

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
          become(processing(children))
          parent ! SetupDone
        } else become(setup(children, remaining - worker))
      }
  }

  def processing(children: Set[ActorRef]): Actor.Receive = {
    case _ =>
  }
}

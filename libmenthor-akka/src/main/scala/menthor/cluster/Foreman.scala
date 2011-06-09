package menthor.akka.cluster

import menthor.akka.processing.{Worker, SetupDone}

import akka.actor.{Actor, ActorRef}
import collection.mutable.ListBuffer

class Foreman[Data: Manifest](val parent: ActorRef) extends Actor {
  var children: List[ActorRef] = Nil

  def receive = {
    case CreateWorkers(count) =>
      children = createWorkers(count)
      self.channel ! WorkersCreated(children)
    case SetupDone =>
      become(processing)
      for (child <- children)
        child ! SetupDone
  }

  def processing: Actor.Receive = {
    case _ =>
  }


  private def createWorkers(count: Int) = {
    val workers = new ListBuffer[ActorRef]
    for (i <- 1 to count)
      workers += Actor.actorOf(new Worker[Data](self)).start()
    workers.toList
  }
}

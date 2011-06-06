package menthor.akka.parallel

import akka.actor.{Actor, ActorRef}

class Worker(val parent: ActorRef) extends Actor {
  def receive = {
    case _ =>
  }
}

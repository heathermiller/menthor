package menthor.akka.cluster

import menthor.akka.parallel.Worker

import akka.actor.{Actor, ActorRef}
import collection.mutable.ListBuffer

class Foreman(val parent: ActorRef) extends Actor {
  def receive = {
    case AvailableProcessors =>
      self.channel ! AvailableProcessors(Runtime.getRuntime.availableProcessors)
    case CreateWorkers(count) =>
      createWorkers(count)
  }

  private def createWorkers(count: Int) {
    var workers = new ListBuffer[ActorRef]
    for (i <- 1 to count)
      workers += Actor.actorOf(new Worker(self)).start()
    self.channel ! WorkersCreated(workers.toList)
  }
}

package menthor.akka

import akka.actor.Actor
import Actor._

/** */
class ClusterService extends Actor {
  def receive = {
    case CreateForeman => {
      val foreman = actorOf[Foreman]
      remote.registerByUuid(foreman)
      self.reply(foreman.uuid.toString)
    }
  }
}

object ClusterService {
  def main(args: Array[String]) {
    remote.start("localhost", 2552)
    remote.register("menthor-cluster-service", actorOf[ClusterService])
  }
}


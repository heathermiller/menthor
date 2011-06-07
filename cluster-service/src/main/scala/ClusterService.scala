package menthor.akka.cluster

import akka.actor.Actor
import Actor._
import akka.serialization.RemoteActorSerialization._

class ClusterService extends Actor {
  def receive = {
    case AvailableProcessors =>
      self.channel ! AvailableProcessors(Runtime.getRuntime.availableProcessors)
    case CreateForeman(parent) =>
      val foreman = actorOf(new Foreman(parent)).start()
      self.channel ! ForemanCreated(foreman)
  }
}

object ClusterService {
  def run() {
    remote.register(actorOf[ClusterService])
    remote.start()
  }

  def main(args: Array[String]) {
    run()
  }
}

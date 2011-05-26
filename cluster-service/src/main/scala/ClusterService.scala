package menthor.akka

import akka.actor.Actor
import Actor._
import akka.serialization.RemoteActorSerialization._

class ClusterService extends Actor {
  def receive = {
    case CreateForeman =>
      val foreman = actorOf[Foreman].start()
      self.channel ! toRemoteActorRefProtocol(foreman).toByteArray
  }

  override def postStop() {
    registry.shutdownAll()
    remote.shutdown()
  }
}

object ClusterService {
  def run() {
    remote.start()
    remote.register("menthor-cluster-service", actorOf[ClusterService])
  }

  def main(args: Array[String]) {
    run()
  }
}


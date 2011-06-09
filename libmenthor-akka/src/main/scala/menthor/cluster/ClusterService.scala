package menthor.cluster

import akka.actor.Actor
import Actor._
import akka.serialization.RemoteActorSerialization._

import java.util.concurrent.CountDownLatch

class ClusterService extends Actor {
  def receive = {
    case AvailableProcessors =>
      self.channel ! AvailableProcessors(Runtime.getRuntime.availableProcessors)
    case msg @ CreateForeman(parent) =>
      implicit val m: Manifest[msg.Data] = msg.manifest
      val foreman = actorOf(new Foreman[msg.Data](parent)).start()
      self.channel ! ForemanCreated(foreman)
  }

  override def postStop() {
    ClusterService.keepAlive.countDown()
  }
}

object ClusterService {
  val keepAlive = new CountDownLatch(1)

  def run() {
    Runtime.getRuntime.addShutdownHook(new Thread( new Runnable {
      def run() {
        remote.shutdown()
        registry.shutdownAll()
      }
    } ) )
    remote.register(actorOf[ClusterService])
    remote.start()
  }

  def main(args: Array[String]) {
    run()
    keepAlive.await
    System.exit(0)
  }
}

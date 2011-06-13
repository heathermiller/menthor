package menthor.cluster

import akka.actor.Actor
import Actor._
import akka.serialization.RemoteActorSerialization._
import akka.remote.{ RemoteServerSettings => Settings }

import java.util.concurrent.CountDownLatch

class ClusterService extends Actor {
  def receive = {
    case AvailableProcessors =>
      self.channel ! AvailableProcessors(Runtime.getRuntime.availableProcessors)
    case msg @ CreateForeman(parent) =>
      implicit val m: Manifest[msg.Data] = msg.manifest
      val supervisor = actorOf[GraphSupervisor].start()
      val foreman = actorOf(new Foreman[msg.Data](parent)).start()
      supervisor.link(foreman)
      self.channel ! ForemanCreated(foreman, supervisor)
  }

  override def postStop() {
    ClusterService.keepAlive.countDown()
  }
}

object ClusterService {
  val keepAlive = new CountDownLatch(1)

  def run(hostname: String = Settings.HOSTNAME, port: Int = Settings.PORT) {
    Runtime.getRuntime.addShutdownHook(new Thread( new Runnable {
      def run() {
        remote.shutdown()
        registry.shutdownAll()
      }
    } ) )
    remote.start(hostname, port)
    remote.register(actorOf[ClusterService])
  }

  def main(args: Array[String]) {
    run()
    keepAlive.await
    sys.exit()
  }
}

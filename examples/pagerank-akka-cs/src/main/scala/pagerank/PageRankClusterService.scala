package pagerank

import akka.actor.Actor.{ actorOf, remote, registry }
import menthor.cluster.ClusterService

object PageRankClusterService {
  def run() {
    Runtime.getRuntime.addShutdownHook(new Thread( new Runnable {
      def run() {
        remote.shutdown()
        registry.shutdownAll()
      }
    } ) )
    remote.start()
    remote.register(actorOf[ClusterService])
  }

  def main(args: Array[String]) {
    run()
    ClusterService.keepAlive.await
    sys.exit()
  }
}

package menthor.akka.cluster

import menthor.akka.processing._

import org.scalatest.fixture.FixtureFunSuite
import scala.sys.SystemProperties
import scala.sys.process.{Process, ProcessBuilder, ProcessLogger}
import java.net.URLClassLoader

import akka.actor.{Actor, ActorRef}
import Actor._
import akka.remoteinterface._
import akka.serialization.RemoteActorSerialization._

class ClusterServiceAppFixture

object ClusterServiceAppFixture extends App {
  val listener = actorOf(new Actor {
      def receive = {
        case x => println(x)
      }
    }).start()

  registry.addListener(listener)
  remote.addListener(listener)
  
  ClusterService.run()
  ClusterService.keepAlive.await
  System.exit(0)
}

class ClusterServiceSuite extends FixtureFunSuite {
  type FixtureParam = ActorRef

  def withFixture(test: OneArgTest) {
    val listener = actorOf(new Actor {
        def receive = {
          case x => println("Remote Listener: " + x)
        }
      }).start()

    remote.addListener(listener)

    // Find the class loader of the ClusterService class, should be a
    // URLClassLoader so that we can get a working ClassPath
    val loader = classOf[ClusterServiceAppFixture].getClassLoader.asInstanceOf[URLClassLoader]
    // Read the system properties to get the information required to run the
    // JVM and construct the ClassPath.
    val prop = new SystemProperties
    val fsep = prop("file.separator")
    val psep = prop("path.separator")
    val path = prop("java.home") + fsep + "bin" + fsep + "java"
    val classPath = loader.getURLs.map(_.getPath).reduceLeft(_ + psep + _)
    val mainClass = classOf[ClusterServiceAppFixture].getCanonicalName
    
    val logger = ProcessLogger((x: String) => println("ClusterService: " + x))
    val process = Process(path, Seq("-cp", classPath, mainClass)).run(logger)

    // Wait for forked JVM
    // TODO find a better way
    Thread.sleep(1000)

    // Get the ActorRef of the ClusterService
    val service = remote.actorFor(classOf[ClusterService].getCanonicalName, "localhost", 2552)
    try {
      test(service)
      if (service.isRunning)
        service.stop()
      assert(process.exitValue() == 0)
    } finally {
      process.destroy()
      remote.removeListener(listener)
      registry.shutdownAll()
    }
  }

  test("cluster service acceptance test") { service =>
    val graph = actorOf[Graph]
    info("getting the number of available processors")
    service !! AvailableProcessors match {
        case Some(AvailableProcessors(_: Int)) =>
        case x => invalidResponse("cluster service", "getting the number of available processors", x)
    }
    info("creating foreman")
    val foreman: ActorRef = service !! CreateForeman[Int](graph) match {
      case Some(ForemanCreated(foremanRef)) => foremanRef
      case x => invalidResponse("cluster service", "creating foreman", x)
    }
    info("creating worker")
    val worker: ActorRef = foreman !! CreateWorkers(1) match {
      case Some(WorkersCreated(List(workerRef))) => workerRef
      case x => invalidResponse("foreman", "creating worker", x)
    }
  }

  def invalidResponse(actorName: String, action: String, response: Any) = response match {
    case None => fail("timeout when contacting " + actorName)
    case Some(response) => fail("unknown response from " + actorName + " when " + action + ":\n" + response)
  }
}


package menthor.akka

import org.scalatest.fixture.FixtureFunSuite
import scala.sys.SystemProperties
import scala.sys.process.{Process, ProcessBuilder, ProcessLogger}
import java.net.URLClassLoader

import akka.actor.{Actor, ActorRef}
import Actor._
import akka.remoteinterface._
import akka.serialization.RemoteActorSerialization._

class ClusterServiceSuite extends FixtureFunSuite {
  type FixtureParam = ActorRef

  val listener = actorOf(new Actor {
      def receive = {
        case x => println("Remote Listener:" + x)
      }
    }).start()

  remote.addListener(listener)


  def withFixture(test: OneArgTest) {
    // Find the class loader of the ClusterService class, should be a
    // URLClassLoader so that we can get a working ClassPath
    val loader = classOf[ClusterService].getClassLoader.asInstanceOf[URLClassLoader]
    // Read the system properties to get the information required to run the
    // JVM and construct the ClassPath.
    val prop = new SystemProperties
    val fsep = prop("file.separator")
    val psep = prop("path.separator")
    val path = prop("java.home") + fsep + "bin" + fsep + "java"
    val classPath = loader.getURLs.map(_.getPath).reduceLeft(_ + psep + _)
    val mainClass = classOf[ClusterService].getCanonicalName
    
    val logger = ProcessLogger((x: String) => println("ClusterService: " + x))
    val process = Process(path, Seq("-cp", classPath, mainClass)).run(logger)

    // Wait for forked JVM
    // TODO find a better way
    Thread.sleep(1000)

    try {
      // Get the ActorRef of the ClusterService
      val service = remote.actorFor("menthor-cluster-service", "localhost", 2552)
      test(service)
      if (! service.isShutdown)
        service.stop()
    } finally {
      // TODO Takes a really long time, timeout somewhere?
      assert(process.exitValue() == 0)
      process.destroy()
    }
  }

  test("create and contact foreman") { service =>
    service !! CreateForeman match {
      case Some(foreman: Array[Byte]) => contact(fromBinaryToRemoteActorRef(foreman))
      case None => fail("timeout when contacting cluster service")
      case Some(response) => fail("unknown response when creating foreman:\n" + response)
    }
  }

  def contact(foreman: ActorRef) = {
    foreman !! "hello" match {
      case Some("world") =>
      case None => fail("timeout when contacting foreman")
      case Some(response) => fail("unknown response from foreman:\n" + response)
    }
  }
}


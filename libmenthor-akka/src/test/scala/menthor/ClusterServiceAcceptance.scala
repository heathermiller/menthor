package menthor

import menthor.processing._
import menthor.io._
import menthor.config._
import menthor.cluster._

import org.scalatest.FunSuite
import scala.sys.SystemProperties
import scala.sys.process.{ Process, ProcessBuilder, ProcessLogger }
import java.net.{ URLClassLoader, InetSocketAddress }
import java.util.concurrent.CountDownLatch

import akka.actor.{ Actor, ActorRef, Uuid }
import akka.actor.Actor._
import akka.agent.Agent
import akka.event.EventHandler
import akka.remote.{ RemoteServerSettings => Settings }

class ClusterServiceAppFixture

object ClusterServiceAppFixture extends App {
  ClusterService.run(port = 2552 + util.Random.nextInt(1000))
  remote.actorFor("init-test-service", Settings.HOSTNAME, 1234) ! remote.address
  ClusterService.keepAlive.await
  sys.exit()
}

class TestVertex(val initialValue: Int) extends Vertex[Int] {
  var success = false

  var _neighbors: List[VertexRef] = Nil

  def connectTo(successor: VertexRef) {
    _neighbors = successor :: _neighbors
  }

  def neighbors = _neighbors

  def update() = {
    if (incoming.nonEmpty) {
      EventHandler.debug(this, "Second step")
      val vertices = (0 until numVertices).toSet
      val values = incoming.map(_.value).toSet
      success = values == vertices
      voteToHalt
    } else {
      EventHandler.debug(this, "First step")
      for (neighbor <- neighbors) yield Message(neighbor, value)
    }
  }

//  def update() = {
//    {
//      for (neighbor <- neighbors) yield Message(neighbor, value)
//    } then {
//      val vertices = (0 until numVertices).toSet
//      var values = incoming.map(_.value).toSet
//      success = values == vertices
//      Nil
//    } crunch(_ + _) then {
//      val sum = (0 until numVertices).sum
//      incoming match {
//        case List(result) => success = success && (sum != result.value)
//        case _ => success = false
//      }
//      voteToHalt
//    }
//  }
}

class TestDataIO extends AbstractDataIO[Int, Int] {
  var verticesPartitions = Map.empty[Uuid, Map[VertexID, List[VertexID]]]
  var workers: List[Uuid] = Nil

  def partitionVertices(topology: List[List[Uuid]]) {
    workers = topology.flatten
    val vids = (0 until workers.size).toList
    for (vid <- vids) {
      verticesPartitions += (workers(vid) -> Map(vid -> vids))
    }
  }

  def numVertices = workers.size
  def vertices(worker: Uuid) = verticesPartitions(worker)
  def workerIO(worker: Uuid) = this
  def ownerUuid(vid: Int) = workers(vid)
  def createVertex(vid: Int) = new TestVertex(vid)

  def processVertices(worker: Uuid, vertices: Iterable[Vertex[Int]]) {
    for (_vertex <- vertices)
      _vertex match { case vertex: TestVertex => processVertex(worker, vertex) }
  }

  def processVertex(worker: Uuid, vertex: TestVertex) {
    remote.actorFor("end-test-service", Settings.HOSTNAME, 1234) ! vertex.success
  }
}

class ClusterServiceAcceptance extends FunSuite {
  test("cluster service acceptance test") {
    val prop = new SystemProperties
    prop += ("akka.mode" -> "debug")

    // Find the class loader of the ClusterService class, should be a
    // URLClassLoader so that we can get a working ClassPath
    val loader = classOf[ClusterServiceAppFixture].getClassLoader.asInstanceOf[URLClassLoader]
    // Read the system properties to get the information required to run the
    // JVM and construct the ClassPath.
    val fsep = prop("file.separator")
    val psep = prop("path.separator")
    val path = prop("java.home") + fsep + "bin" + fsep + "java"
    val classPath = loader.getURLs.map(_.getPath).reduceLeft(_ + psep + _)
    val mainClass = classOf[ClusterServiceAppFixture].getCanonicalName

    val servicesReady = new CountDownLatch(0)
    val servicesAddress: Agent[List[InetSocketAddress]] = Agent(Nil)
    remote.start(Settings.HOSTNAME, 1234)
    val initService = actorOf(new Actor {
      def receive = {
        case address: InetSocketAddress =>
          servicesAddress send (address :: _)
          servicesReady.countDown()
      }
    } )
    remote.register("init-test-service", initService)
    val logger = ProcessLogger((x: String) => println("ClusterService: " + x))
    val processes = List.fill(0)(Process(path, Seq("-cp", classPath, mainClass)).run(logger))

    // Wait for the ready signal from the cluster service
    servicesReady.await
    remote.unregister("init-test-service")
    initService.stop()

    val addresses = servicesAddress.await
    val conf = new Config {
      override val localWorkers = 2
      override val configuration = addresses.map { address =>
        (address.getHostName, Some(address.getPort), Some((WorkerModifier.Absolute, 2)))
      }
    }

    val nodeCount = if (conf.configuration.size != 0) conf.configuration.size else 1
    val successful = Agent(true)
    val finished = new CountDownLatch(nodeCount * 2)
    val endService = actorOf(new Actor {
      def receive = {
        case success: Boolean =>
          successful send (_ && success)
          finished.countDown()
      }
    } )
    remote.register("end-test-service", endService)
    
    val master = actorOf(new GraphMaster(new TestDataIO, conf)).start()

    finished.await
    val success = successful.await
    remote.unregister("end-test-service")
    endService.stop()
    for (address <- addresses)
      remote.actorFor(classOf[ClusterService].getCanonicalName, address.getHostName, address.getPort).stop()
    for (process <- processes)
      process.exitValue()

    remote.shutdown()
    registry.shutdownAll()
    assert(success)
  }
}


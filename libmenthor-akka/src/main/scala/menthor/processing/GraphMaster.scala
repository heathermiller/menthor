package menthor.processing

import menthor.config.{Config, WorkerModifier}
import menthor.cluster.{ClusterService, AvailableProcessors}
import menthor.cluster.{CreateForeman, ForemanCreated}
import menthor.cluster.{CreateWorkers, WorkersCreated}
import menthor.cluster.{GraphSupervisor, GraphSupervisors}
import menthor.io.DataIO

import akka.actor.{Actor, ActorRef, Uuid}
import akka.agent.Agent
import akka.dispatch.Future
import akka.remote.{RemoteServerSettings, RemoteClientSettings}

import java.util.concurrent.CountDownLatch

class GraphMaster[Data: Manifest](val dataIO: DataIO[Data], _conf: Option[Config] = None) extends GraphSupervisor {
  def this(dataIO: DataIO[Data], _conf: Config) = this(dataIO, Some(_conf))

  private lazy val conf = _conf getOrElse new Config

  private lazy val topology = createTopology

  def verticesCreation(remaining: Int): Receive = {
    case VerticesCreated =>
      if (remaining > 1) become(verticesCreation(remaining - 1))
      else {
        val workers = topology._2.flatMap(_.values)
        become(verticesSharing(workers.size) orElse super.receive)
        for (worker <- workers)
          worker ! ShareVertices
      }
  }

  def verticesSharing(remaining: Int): Receive = {
    case VerticesShared =>
      if (remaining > 1) become(verticesSharing(remaining - 1))
      else {
        val workers = topology._2.flatMap(_.values)
        unbecome()
        for (worker <- workers)
          worker ! SetupDone
      }
  }

  private def postStart {
    val (graph, workers) = topology

    dataIO.useWorkers(workers)

    val flatWorkers = workers.flatten

    become(verticesCreation(flatWorkers.size) orElse super.receive)

    for ((uuid, worker) <- flatWorkers) 
      worker ! CreateVertices(dataIO.workerIO(uuid))
  }

  override def preStart {
    become {
      case "PostStart" => postStart
    }
    self ! "PostStart"
  }

  private def createTopology: (ActorRef, List[Map[Uuid, ActorRef]]) = {
    if (conf.configuration.isEmpty) createLocalTopology
    else createClusterTopology
  }

  private def createLocalTopology = {
    val workerCount = conf.localWorkers
    if (workerCount == 0)
      sys.exit(1)

    val graph = Actor.actorOf(new Graph(workerCount)).start()
      self.link(graph)

    val workers = Seq.fill(workerCount) {
      val worker = Actor.actorOf(new Worker(graph)).start()
      self.link(worker)
      worker.uuid -> worker
    }
    graph -> List(Map(workers: _*))
  }
    
  private def createClusterTopology = {
    val nodesInfo = Future.sequence {
      for {
        (hostname, _port, _workerCount) <- conf.configuration
        port = _port getOrElse RemoteServerSettings.PORT
        service = Actor.remote.actorFor(classOf[ClusterService].getCanonicalName, hostname, port)
      } yield {
        service !!! AvailableProcessors map { procMsg: Any =>
          procMsg match {
            case AvailableProcessors(proc) =>
              val workerCount = _workerCount match {
                case Some((WorkerModifier.Absolute, n)) => n
                case Some((WorkerModifier.Times, n)) => n * proc
                case Some((WorkerModifier.Plus, n)) => proc + n
                case Some((WorkerModifier.Minus, n)) => proc - n
                case None => proc
              }
              service -> workerCount
          }
        } failure {
          case _ => service -> 0
        }
      }
    }.get.filter(_._2 > 0)
    
    if (nodesInfo.isEmpty)
      sys.exit(-1)

    val graph = Actor.actorOf(new Graph(nodesInfo.size)).start()
    self.link(graph)

    val supervisors: Agent[Map[Uuid,ActorRef]] = Agent(Map.empty)

    val workersFuture = Future.sequence {
      for {
        (service, workerCount) <- nodesInfo
      } yield {
        service !!! CreateForeman(graph) flatMap { foremanMsg: Any =>
          val (foreman, (suuid, supervisor)) = foremanMsg match { case ForemanCreated(f, s) => (f, s) }
          supervisors send (_ + (suuid -> supervisor))
          foreman !!! CreateWorkers(workerCount) map { workersMsg: Any =>
            workersMsg match { case WorkersCreated(m) => m }
          }
        }
      }
    }

    workersFuture.await

    val fsupervisors = supervisors.await
    graphSupervisors = fsupervisors.values.toList
    for (s <- graphSupervisors)
      s ! GraphSupervisors(fsupervisors + (self.uuid -> self))

    graph -> workersFuture.get
  }
}

package menthor.processing

import menthor.config.{Config, WorkerModifier}
import menthor.cluster.{ClusterService, AvailableProcessors}
import menthor.cluster.{CreateForeman, ForemanCreated}
import menthor.cluster.{CreateWorkers, WorkersCreated}
import menthor.cluster.{GraphSupervisor, GraphCFMs, ClusterFailureMonitor}
import menthor.io.DataIOMaster

import akka.actor.{Actor, ActorRef, Uuid}
import akka.agent.Agent
import akka.dispatch.Future
import akka.remote.{RemoteServerSettings, RemoteClientSettings}

import java.util.concurrent.CountDownLatch

class GraphMaster[Data: Manifest](val dataIO: DataIOMaster[Data], _conf: Option[Config] = None) extends GraphSupervisor {
  def this(dataIO: DataIOMaster[Data], _conf: Config) = this(dataIO, Some(_conf))

  _someCfm = Some(Actor.actorOf[ClusterFailureMonitor].start())

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

  override def postStop {
    super.postStop()
    Actor.remote.shutdown()
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
    Actor.remote.start()
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

    val supervisors: Agent[Map[ActorRef,(Uuid,ActorRef)]] = Agent(Map.empty)

    val workersFuture = Future.sequence {
      for {
        (service, workerCount) <- nodesInfo
      } yield {
        service !!! CreateForeman(graph) flatMap { foremanMsg: Any =>
          val (foreman, (supervisor, muuid, monitor)) = foremanMsg match { case ForemanCreated(f, s) => (f, s) }
          supervisors send (_ + (supervisor -> (muuid -> monitor)))
          foreman !!! CreateWorkers(workerCount) map { workersMsg: Any =>
            workersMsg match { case WorkersCreated(m) => m }
          }
        }
      }
    }

    workersFuture.await

    val fsupervisors = supervisors.await

    val monitors = GraphCFMs(Map(fsupervisors.values.toSeq: _*) + (cfm.uuid -> cfm))

    for (supervisor <- fsupervisors.keys)
      supervisor ! monitors

    graph -> workersFuture.get
  }
}

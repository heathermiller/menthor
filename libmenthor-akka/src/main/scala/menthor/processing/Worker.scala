package menthor.processing

import menthor.MenthorException
import menthor.io.DataIO

import akka.actor.{Actor, ActorRef, LocalActorRef, Channel, Uuid, Unlink}
import akka.config.Supervision.Temporary
import akka.event.EventHandler

import collection.mutable

class VerticesInitializationException private[menthor](message: String, cause: Throwable = null) extends MenthorException(message, cause)

object Worker {
  val vertexCreator = new ThreadLocal[Option[Worker[_]]] {
    override def initialValue = None
  }
}

class Worker[Data: Manifest](val parent: ActorRef) extends Actor {
  val lifeCycle = Temporary

  private var vertices = Map.empty[Uuid, Vertex[Data]]

  private var carryOn = Set.empty[Uuid]

  private var _totalNumVertices: Int = 0

  private var _superstep: Superstep = 0
  
  private var output: DataIO[Data] = null

  private val mailbox = new mutable.HashMap[Uuid, List[Message[Data]]] {
    override def default(v: Uuid) = Nil
  }

  def totalNumVertices = _totalNumVertices

  def superstep: Superstep = _superstep

  def incoming(vUuid: Uuid) = mailbox(vUuid)

  def voteToHalt(vUuid: Uuid) { carryOn -= vUuid }

  def receive = {
    case CreateVertices(dataIO) =>
      assert(Worker.vertexCreator.get.isEmpty)
      Worker.vertexCreator.set(Some(this))
      try {
        createVertices(dataIO)
      } catch {
        case e => throw new VerticesInitializationException(
          "DataIO module failed to create the vertices", e
        )
      }
      Worker.vertexCreator.remove()
      self.channel ! VerticesCreated
  }

  def createVertices(dataIO: DataIO[Data]) {
    implicit val vidmanifest = dataIO.vidmanifest

    _totalNumVertices = dataIO.numVertices
    val subgraph = dataIO.vertices(self.uuid)
    var knownVertices = Map.empty[dataIO.VertexID, VertexRef]
    var unknownVertices = Set.empty[dataIO.VertexID]
    for ((vid, neighbors) <- subgraph) {
      val vertex = dataIO.createVertex(vid)
      vertices += (vertex.ref.uuid -> vertex)
      knownVertices += (vid -> vertex.ref)
      unknownVertices ++= neighbors
    }
    unknownVertices --= knownVertices.keys

    output = dataIO
    become(share(dataIO.owner, knownVertices, unknownVertices, subgraph))
  }

  def share[VertexID: Manifest, Data](
    owner: VertexID => ActorRef,
    known: Map[VertexID, VertexRef],
    unknown: Set[VertexID],
    subgraph: Map[VertexID, Iterable[VertexID]],
    controlChannel: Option[Channel[Any]] = None
  ): Actor.Receive = {
    case ShareVertices =>
      if (unknown.isEmpty)
        self.channel ! VerticesShared
      else {
        become(share(owner, known, unknown, subgraph, Some(self.channel)))
        for (vid <- unknown)
          try {
            owner(vid) ! RequestVertexRef(vid)
          } catch {
            case e => throw new VerticesInitializationException(
              "Failed to send a request for the VertexRef of vertex [" + vid +"]", e
            )
          }
      }
    case msg @ VertexRefForID(_vid, vUuid, wUuid) if msg.manifest == manifest[VertexID] =>
      val vid = _vid.asInstanceOf[VertexID]
      val workerRef = Actor.registry.actorFor(wUuid) orElse self.sender
      become(share(owner, known + (vid -> new VertexRef(Some(vUuid),
        workerRef)), unknown - vid, subgraph, controlChannel))

      if ((unknown - vid).isEmpty && controlChannel.isDefined)
        controlChannel.get ! VerticesShared
    case msg @ RequestVertexRef(_vid) if msg.manifest == manifest[VertexID] =>
      val vid = _vid.asInstanceOf[VertexID]
      self.channel ! VertexRefForID(vid, known(vid).uuid, self.uuid)
    case SetupDone =>
      for ((vid, neighbors) <- subgraph) {
        val vertex = vertices(known(vid).uuid)
        for (nid <- neighbors)
          vertex.connectTo(known(nid))
      }
      become(processing)
      parent ! SetupDone
  }

  def processing: Actor.Receive = {
    case _msg @ Message(dest, _) if ((_msg.step == superstep) && (vertices contains dest.uuid)) =>
      val msg = _msg.asInstanceOf[Message[Data]]
      mailbox(dest.uuid) = msg :: mailbox(dest.uuid)
    case TransmitMessage(dest, _value, source, step) if ((step == superstep) && (vertices contains dest)) =>
      assert(self.sender.isDefined)
      val value = _value.asInstanceOf[Data]
      val msg = Message(vertices(dest).ref, value)(new VertexRef(Some(source), Some(self.sender.get)), step)
      mailbox(dest) = msg :: mailbox(dest)
    case Stop =>
      EventHandler.info(this, "Processing results")
      output.processVertices(self.uuid, vertices.values)
      self.stop()
    case Next =>
      nextstep()
  }

  def crunchResult: Actor.Receive = {
    case CrunchResult(_result) =>
      val result = _result.asInstanceOf[Data]
      for ((uuid, vertex) <- vertices) {
        val msg = Message(vertex.ref, result)(null, superstep)
        mailbox(uuid) = msg :: mailbox(uuid)
      }
      unbecome()
      nextstep()
  }

  private def nextstep() {
    _superstep += 1

    var crunchsteps: List[CrunchStep[Data]] = Nil
    var substeps: List[Substep[Data]] = Nil

    for (vertex <- vertices.values) vertex.currentStep match {
      case crunchstep: CrunchStep[_] =>
        crunchsteps = crunchstep.asInstanceOf[CrunchStep[Data]] :: crunchsteps
      case substep: Substep[_] =>
        substeps = substep.asInstanceOf[Substep[Data]] :: substeps
    }
    if (crunchsteps.nonEmpty && substeps.nonEmpty) {
      throw new InvalidStepException("Mixing crunches and substeps at step [" + superstep + "]")
    } else if (crunchsteps.nonEmpty) {
      val cruncher = crunchsteps.head.cruncher
      for (step <- crunchsteps.tail)
        if (step.cruncher != cruncher)
          throw new InvalidStepException("Different crunchers are used at step [" + superstep + "]")

      try {
        val result = vertices.values.map(_.value) reduceLeft cruncher
        parent ! Crunch(cruncher, result)
      } catch {
        case e => throw new ProcessingException("Cruncher application error at step [" + superstep + "]", e)
      }
      become(crunchResult, false)
    } else if (substeps.nonEmpty) {
      val validStep = substeps forall { substep =>
        ! (substep.cond.isDefined && substep.cond.get())
      }
      if (validStep) {
        carryOn = vertices.keySet
        var outgoing: List[Message[Data]] = Nil

        try {
          for (substep <- substeps)
            outgoing ::: substep.stepfun()
        } catch {
          case e => throw new ProcessingException("Substep execution error at step [" + superstep + "]", e)
        }

        mailbox.clear()

        for (msg <- outgoing) msg.dest.worker match {
          case w if (w == self) =>
            mailbox(msg.dest.uuid) = msg :: mailbox(msg.dest.uuid)
          case w: LocalActorRef =>
            w ! msg
          case w: ActorRef =>
            w ! TransmitMessage(msg.dest.uuid, msg.value, msg.source.uuid, msg.step)
        }
        val status = if (carryOn.isEmpty) Halt else Done
        parent ! status
      } else {
        for (vertex <- vertices.values)
          vertex.moveToNextStep()
        _superstep -= 1
        nextstep()
      }
    }
    for (vertex <- vertices.values)
      vertex.moveToNextStep()
  }
}

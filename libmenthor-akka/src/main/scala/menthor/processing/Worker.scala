package menthor.processing

import menthor.io.{DataInput, DataOutput}

import akka.actor.{Actor, ActorRef, LocalActorRef, Channel, Uuid}
import collection.mutable

object Worker {
  val vertexCreator = new ThreadLocal[Option[Worker[_]]] {
    override def initialValue = None
  }
}

class Worker[Data: Manifest](val parent: ActorRef) extends Actor {
  private var vertices = Map.empty[Uuid, Vertex[Data]]

  private var carryOn = Set.empty[Uuid]

  private var _totalNumVertices: Int = 0

  private var _superstep: Superstep = 0

  private val mailbox = new mutable.HashMap[Uuid, List[Message[Data]]] {
    override def default(v: Uuid) = Nil
  }

  def totalNumVertices = _totalNumVertices

  def superstep: Superstep = _superstep

  def incoming(vUuid: Uuid) = mailbox(vUuid)

  def voteToHalt(vUuid: Uuid) { carryOn - vUuid }

  def receive = {
    case CreateVertices(source) =>
      Worker.vertexCreator.get match {
        case Some(_) => throw new IllegalStateException("A worker is already creating vertices")
        case None => Worker.vertexCreator.set(Some(this))
      }
      createVertices(source)
      Worker.vertexCreator.remove()
      self.channel ! VerticesCreated
  }

  def createVertices(source: DataInput[Data]) {
    implicit val vidmanifest = source.vidmanifest

    _totalNumVertices = source.numVertices
    val subgraph = source.vertices(self)
    var knownVertices = Map.empty[source.VertexID, VertexRef]
    var unknownVertices = Set.empty[source.VertexID]
    for ((vid, neighbors) <- subgraph) {
      val vertex = source.createVertex(vid)
      vertices += (vertex.ref.uuid -> vertex)
      knownVertices += (vid -> vertex.ref)
      unknownVertices ++= neighbors
    }
    unknownVertices --= knownVertices.keys

    become(share(source.owner _, knownVertices, unknownVertices, subgraph))
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
          owner(vid) ! RequestVertexRef(vid)
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
    case TransmitMessage(dest, _value, source, step) if ((step == superstep) && (vertices contains dest)) => {
      assert(self.sender.isDefined)
      val value = _value.asInstanceOf[Data]
      val msg = Message(vertices(dest).ref, value)(new VertexRef(Some(source), Some(self.sender.get)), step)
      mailbox(dest) = msg :: mailbox(dest)
    }
    case Stop =>
      become(processResults)
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
      throw new IllegalStateException("Vertices mix crunches and substeps in the same step")
    } else if (crunchsteps.nonEmpty) {
      val cruncher = crunchsteps.head.cruncher
      for (step <- crunchsteps.tail)
        assert(step.cruncher == cruncher)

      val result = vertices.values.map(_.value) reduceLeft cruncher
      become(crunchResult)
      parent ! Crunch(cruncher, result)
    } else if (substeps.nonEmpty) {
      val validStep = substeps forall { substep =>
        ! (substep.cond.isDefined && substep.cond.get())
      }
      if (validStep) {
        carryOn = vertices.keySet
        var outgoing: List[Message[Data]] = Nil

        for (substep <- substeps)
          outgoing ::: substep.stepfun()

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
  }

  def processResults: Actor.Receive = {
    case ProcessResults(output) =>
      output.process(vertices.values)
      self.stop()
  }
}

package menthor.processing

import menthor.MenthorException
import menthor.io.DataIO

import akka.actor.{Actor, ActorRef, LocalActorRef, Channel, Uuid, Unlink}
import akka.config.Supervision.Temporary

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

  private var futureMessages: List[Message[Data]] = Nil

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
    val subgraph = Map(dataIO.vertices(self.uuid).toSeq: _*)
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
      assert(self.sender.isDefined)
      val vid = _vid.asInstanceOf[VertexID]
      val workerRef = Actor.registry.actorFor(wUuid) getOrElse self.sender.get
      val vref = new VertexRef(vUuid, wUuid, workerRef)
      become(share(owner, known + (vid -> vref), unknown - vid, subgraph,
        controlChannel))

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
      for (vertex <- vertices.values)
        vertex.initialize
      become(processing(0))
      parent ! SetupDone(Set(self.uuid))
  }

  def processing(remaining: Int): Actor.Receive = {
    case msg@DataMessage(step, duuid) if ((step >= superstep) && (vertices contains duuid)) =>
      assert(self.sender.isDefined)
      val message = msg match {
        case Message(dest, _) =>
          msg.asInstanceOf[Message[Data]]
        case TransmitMessage(dest, _value, (vuuid, wuuid), _) =>
          val value = _value.asInstanceOf[Data]
          Message(vertices(dest).ref, value)(new VertexRef(vuuid, wuuid, self.sender.get), step)
      }
      if (step == superstep) {
        mailbox(duuid) = message :: mailbox(duuid)
        if (remaining - 1 == 0) nextstep()
        else become(processing(remaining - 1))
      } else {
        futureMessages = message :: futureMessages
      }
    case Stop(info) =>
      output.processVertices(self.uuid, vertices.values)
      mailbox.clear

      val rem = remaining + info.getOrElse(self.uuid, 0)
      if (rem == 0) self.stop()
      else become(dropMessages(rem))
    case Next(info) =>
      val rem = remaining + info.getOrElse(self.uuid, 0)
      if (rem == 0) nextstep()
      else become(processing(rem))
    case CrunchResult(_) if vertices.isEmpty =>
      nextstep()
  }

  def dropMessages(remaining: Int): Actor.Receive = {
    case _msg @ Message(dest, _) if ((_msg.step == superstep) && (vertices contains dest.uuid)) =>
      if (remaining - 1 == 0) self.stop()
      else become(dropMessages(remaining - 1))
    case TransmitMessage(dest, _, _, step) if ((step == superstep) && (vertices contains dest)) =>
      if (remaining - 1 == 0) self.stop()
      else become(dropMessages(remaining - 1))
  }

  def crunchResult: Actor.Receive = {
    case CrunchResult(_result) =>
      val result = _result.asInstanceOf[Data]
      for ((uuid, vertex) <- vertices) {
        val msg = Message(vertex.ref, result)(null, superstep)
        mailbox(uuid) = msg :: mailbox(uuid)
      }
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
      become(crunchResult)
      mailbox.clear()
      val cruncher = crunchsteps.head.cruncher
      try {
        val result = vertices.values.map(_.value) reduceLeft cruncher
        parent ! Crunch(cruncher, result)
      } catch {
        case e => throw new ProcessingException("Cruncher application error at step [" + superstep + "]", e)
      }
      for (vertex <- vertices.values)
        vertex.moveToNextStep()
    } else if (substeps.nonEmpty) {
      val validStep = substeps forall { substep =>
        ! (substep.cond.isDefined && substep.cond.get())
      }
      if (validStep) {
        carryOn = Set(vertices.keys.toSeq: _*)
        var outgoing: List[Message[Data]] = Nil

        try {
          for (substep <- substeps)
            outgoing = outgoing ::: substep.stepfun()
        } catch {
          case e => throw new ProcessingException("Substep execution error at step [" + superstep + "]", e)
        }

        mailbox.clear()

        val postInfo = Map(outgoing.groupBy(_.dest.wuuid).mapValues(_.size).toSeq: _*) - self.uuid

        for (msg <- outgoing) msg.dest.worker match {
          case w if (w == self) =>
            mailbox(msg.dest.uuid) = msg :: mailbox(msg.dest.uuid)
          case w: LocalActorRef =>
            w ! msg
          case w: ActorRef =>
            w ! TransmitMessage(msg.dest.uuid, msg.value, (msg.source.uuid, self.uuid), msg.step)
        }

        val (stepMessages, nextStepsMessages) = futureMessages.partition(_.step == superstep)
        for (msg <- stepMessages)
          mailbox(msg.dest.uuid) = msg :: mailbox(msg.dest.uuid)
        futureMessages = nextStepsMessages

        become(processing(- stepMessages.size))

        val status = if (carryOn.isEmpty) Halt(postInfo) else Done(postInfo)
        parent ! status
        for (vertex <- vertices.values)
          vertex.moveToNextStep()
      } else {
        for (vertex <- vertices.values)
          vertex.moveToNextStep()
        _superstep -= 1
        nextstep()
      }
    } else {
      become(processing(0))
      parent ! NothingToDo
      for (vertex <- vertices.values)
        vertex.moveToNextStep()
    }
  }
}

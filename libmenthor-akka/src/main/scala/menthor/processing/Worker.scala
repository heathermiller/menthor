package menthor.processing

import menthor.datainput.DataInput

import akka.actor.{Actor, ActorRef, Channel, Uuid}

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

  def totalNumVertices = _totalNumVertices

  def superstep: Superstep = _superstep

  def incoming(vUuid: Uuid) = Nil

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
    case _ =>
  }
}

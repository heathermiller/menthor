package menthor.processing

import menthor.datainput.DataInput

import akka.actor.{Actor, ActorRef, Channel, Uuid}
import scala.collection.mutable

object Worker {
  val vertexCreator = new ThreadLocal[Option[ActorRef]] {
    override def initialValue = None
  }
}

class Worker[Data: Manifest](val parent: ActorRef) extends Actor {
  val vertices = mutable.Map.empty[Uuid, Vertex[Data]]

  def superstep = 0

  def incoming(vertex: Vertex[Data]) = Nil

  def receive = {
    case CreateVertices(source) =>
      createVertices(source)
      Worker.vertexCreator.get match {
        case Some(_) => throw new IllegalStateException("A worker is already creating vertices")
        case None => Worker.vertexCreator.set(Some(self))
      }
      self.channel ! VerticesCreated
      Worker.vertexCreator.remove()
  }

  def createVertices(source: DataInput[Data]) {
    implicit val vidmanifest = source.vidmanifest

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

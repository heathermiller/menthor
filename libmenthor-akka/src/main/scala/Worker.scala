package menthor.akka.processing

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
    val knownVertices = mutable.Map.empty[source.VertexID, VertexRef]
    val unknownVertices = mutable.Set.empty[source.VertexID]
    for ((vid, neighbors) <- subgraph) {
      val vertex = source.createVertex(vid)
      vertices += (vertex.ref.uuid -> vertex)
      knownVertices += (vid -> vertex.ref)
      unknownVertices ++= neighbors
    }
    unknownVertices --= knownVertices.keys

    var channel: Option[Channel[Any]] = None

    def share: Actor.Receive = {
      case ShareVertices =>
        if (unknownVertices.isEmpty)
          self.channel ! VerticesShared
        else {
          channel = Some(self.channel)
          for (vid <- unknownVertices)
            source.owner(vid) ! RequestVertexRef(vid)
        }
      case msg @ VertexRefForID(_vid, ref) if msg.manifest == manifest[source.VertexID] =>
        val vid = _vid.asInstanceOf[source.VertexID]
        knownVertices += (vid -> ref)
        unknownVertices -= vid
        if (unknownVertices.isEmpty && channel.isDefined)
          channel.get ! VerticesShared
      case msg @ RequestVertexRef(_vid) if msg.manifest == manifest[source.VertexID] =>
        val vid = _vid.asInstanceOf[source.VertexID]
        self.channel ! VertexRefForID(vid, knownVertices(vid))
      case SetupDone =>
        for ((vid, neighbors) <- subgraph) {
          val vertex = vertices(knownVertices(vid).uuid)
          for (nid <- neighbors)
            vertex.connectTo(knownVertices(nid))
        }
        become(processing)
        parent ! SetupDone
    }
    become(share)
  }

  def processing: Actor.Receive = {
    case _ =>
  }
}

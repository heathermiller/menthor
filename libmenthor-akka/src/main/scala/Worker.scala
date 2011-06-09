package menthor.akka.processing

import akka.actor.{Actor, ActorRef, Channel, Uuid}
import scala.collection.mutable

class Worker[Data: Manifest](val parent: ActorRef) extends Actor {
  val vertices = mutable.Map.empty[Uuid, Vertex[Data]]

  def superstep = 0

  def incoming(vertex: Vertex[Data]) = Nil

  def receive = {
    case CreateVertices(source) =>
      createVertices(source)
      self.channel ! VerticesCreated
  }

  def createVertices(source: DataInput[Data]) {
    implicit val vidmanifest = source.vidmanifest
    val knownVertices = mutable.Map.empty[source.VertexID, VertexRef]
    val unknownVertices = mutable.Set.empty[source.VertexID]
    // Compute known/unknown vertices from source
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
        knownVertices(vid) = ref
        unknownVertices -= vid
        if (unknownVertices.isEmpty && channel.isDefined)
          channel.get ! VerticesShared
      case msg @ RequestVertexRef(_vid) if msg.manifest == manifest[source.VertexID] =>
        val vid = _vid.asInstanceOf[source.VertexID]
        self.channel ! VertexRefForID(vid, knownVertices(vid))
      case SetupDone =>
        // Add neighbours to vertices
        become(processing)
        // Notify graph that we are ready for processing
    }
    become(share)
  }

  def processing: Actor.Receive = {
    case _ =>
  }
}

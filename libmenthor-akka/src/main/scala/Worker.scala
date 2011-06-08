package menthor.akka.processing

import akka.actor.{Actor, ActorRef, Uuid}
import scala.collection.mutable

class Worker[Data: Manifest](val parent: ActorRef) extends Actor {
  val vertices = mutable.Map.empty[Uuid, Vertex[Data]]

  def superstep = 0

  def incoming(vertex: Vertex[Data]) = Nil

  def receive = {
    case SetupDone =>
      become(processing)
    case CreateVertices(source) =>
      println(source.vidManifest)
      self.channel ! VerticesCreated
  }

  def processing: Actor.Receive = {
    case _ =>
  }
}

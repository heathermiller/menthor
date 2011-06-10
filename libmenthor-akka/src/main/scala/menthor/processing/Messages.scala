package menthor.processing

import menthor.datainput.DataInput

import akka.actor.Uuid

case class Message[Data](dest: VertexRef, value: Data)(
  implicit val source: VertexRef,
  implicit val step: Int
)

sealed abstract class SetupMessage extends Serializable

case object SetupDone extends SetupMessage

class CreateVertices[Data](val source: DataInput[Data])(
  implicit val manifest: Manifest[Data]
) extends SetupMessage

object CreateVertices {
  def apply[Data: Manifest](source: DataInput[Data]) =
    new CreateVertices(source)

  def unapply[Data: Manifest](msg: CreateVertices[_]): Option[DataInput[Data]] = {
    if ((msg eq null) || (msg.manifest != manifest[Data])) None
    else Some(msg.asInstanceOf[CreateVertices[Data]].source)
  }
}

case object VerticesCreated extends SetupMessage

case object ShareVertices extends SetupMessage

case object VerticesShared extends SetupMessage

case class RequestVertexRef[VertexID](vid: VertexID)(
  implicit val manifest: Manifest[VertexID]
)

case class VertexRefForID[VertexID](vid: VertexID, vertexUuid: Uuid, workerUuid: Uuid)(
  implicit val manifest: Manifest[VertexID]
)

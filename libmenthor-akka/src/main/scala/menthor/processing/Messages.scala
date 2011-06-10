package menthor.processing

import menthor.datainput.DataInput

import akka.actor.Uuid

sealed abstract class InternalMessage

sealed abstract class ControlMessage extends InternalMessage

case object Stop extends ControlMessage

case object Next extends ControlMessage

object WorkerStatusMessage {
  def reduceStatusMessage(ctrl1: WorkerStatusMessage, ctrl2: WorkerStatusMessage) = (ctrl1, ctrl2) match {
    case (StopRequest, _) | (_, StopRequest) => StopRequest
    case (Done, _) | (_, Done) => Done
    case (Halt, Halt) => Halt
  }
}

sealed abstract class WorkerStatusMessage extends InternalMessage

case object StopRequest extends WorkerStatusMessage

case object Done extends WorkerStatusMessage

case object Halt extends WorkerStatusMessage

sealed abstract class CrunchMessage extends InternalMessage

object Crunch {
  def reduceCrunch[Data](crunch1: Crunch[Data], crunch2: Crunch[Data]) = {
    assert(crunch1.cruncher == crunch2.cruncher)
    Crunch(crunch1.cruncher, crunch1.cruncher(crunch1.result, crunch2.result))
  }
}

case class Crunch[Data](cruncher: (Data, Data) => Data, result: Data) extends CrunchMessage

case class CrunchResult[Data](result: Data) extends CrunchMessage

case class Message[Data](dest: VertexRef, value: Data)(
  implicit val source: VertexRef,
  implicit val step: Superstep
)

case class TransmitMessage[Data](dest: Uuid, value: Data, source: Uuid, step: Superstep)

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
) extends SetupMessage

case class VertexRefForID[VertexID](vid: VertexID, vertexUuid: Uuid, workerUuid: Uuid)(
  implicit val manifest: Manifest[VertexID]
) extends SetupMessage

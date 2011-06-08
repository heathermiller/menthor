package menthor.akka.processing

case class Message[Data](dest: VertexRef, value: Data)(
  implicit val source: VertexRef,
  implicit val step: Int
)

sealed abstract class SetupMessage extends Serializable

case object SetupDone extends SetupMessage

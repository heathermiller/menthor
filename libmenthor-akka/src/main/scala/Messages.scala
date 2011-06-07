package menthor.akka.processing

case class Message[Data](source: VertexRef, dest: VertexRef, value: Data) {
  var step: Int = 0
}

case object SetupDone

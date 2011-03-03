package processing.parallel


// step: the step in which the message was produced
// TODO: for distribution need to change Vertex[Data] into vertex ID
case class Message[Data](val source: Vertex[Data], val dest: Vertex[Data], val value: Data) {
  // TODO: step does not need to be visible in user code!
  var step: Int = 0
}

case class Crunch[Data](val cruncher: (Data, Data) => Data, val crunchResult: Data)

case class CrunchResult[Data](res: Data)

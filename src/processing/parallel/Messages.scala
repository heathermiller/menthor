package processing.parallel


// step: the step in which the message was produced
// TODO: for distribution need to change Vertex[Data] into vertex ID
case class Message[Data](val source: Vertex[Data], val dest: Vertex[Data], val value: Data) {
  // TODO: step does not need to be visible in user code!
  var step: Int = 0
}



abstract class AbstractCrunch[Data]

case class Crunch[Data](cruncher: (Data, Data) => Data, crunchResult: Data) extends AbstractCrunch[Data]

case class CrunchToOne[Data](cruncher: (Data, Data) => Data, crunchResult: Data) extends AbstractCrunch[Data]

abstract class AbstractCrunchResult[Data]

case class CrunchResult[Data](res: Data) extends AbstractCrunchResult[Data]

case class CrunchToOneResult[Data](res: Data) extends AbstractCrunchResult[Data]

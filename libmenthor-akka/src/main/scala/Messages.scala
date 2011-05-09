package menthor.akka

import java.util.concurrent.CountDownLatch

// step: the step in which the message was produced
// TODO: for distribution need to change Vertex[Data] into vertex ID
case class Message[Data](val source: Vertex[Data], val dest: Vertex[Data], val value: Data) {
  /* Superstep in which message was created.
   * TODO: step does not need to be visible in user code!
   */
  var step: Int = 0
}

case class Crunch[Data](val cruncher: (Data, Data) => Data, val crunchResult: Data)

case class CrunchResult[Data](res: Data)

// Message type to indicate to graph that it should start propagation
case class StartPropagation(numIterations: Int, latch: CountDownLatch)

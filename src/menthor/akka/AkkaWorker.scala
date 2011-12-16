package menthor.akka

import scala.collection.mutable.{ HashMap, Queue }
import akka.actor.{ Actor, ActorRef }
import akka.actor.Actor.actorOf
import scala.collection.parallel.mutable.ParArray
import scala.collection.GenSeq

/**
 * @param graph      the graph for which the worker manages a partition of vertices
 * @param partition  the list of vertices that this worker manages
 */
class Worker[Data](parent: ActorRef, partition: GenSeq[Vertex[Data]], global: Graph[Data]) extends Actor {
  val queue = new Queue[Message[Data]]
  val later = new Queue[Message[Data]]
  var numSubsteps = 0
  var step = 0
  var id = Graph.nextId

  var incoming = new HashMap[Vertex[Data], List[Message[Data]]]() {
    override def default(v: Vertex[Data]) = List()
  }

  /**
   * Perform a superstep on the data held by this worker.
   */
  def superstep() {
    // remove all application-level messages from mailbox
    while (!queue.isEmpty) {
      val msg = queue.dequeue()
      if (msg.step == step)
        incoming(msg.dest) = msg :: incoming(msg.dest)
      else
        later += msg
    }
    later foreach { m => queue += m }
    later.clear()

    step += 1 // beginning of next superstep

    var allOutgoing: List[Message[Data]] = List()
    var crunch: Option[Crunch[Data]] = None
    var outgoingPerVertex: Array[List[Message[Data]]] = Array.fill[List[Message[Data]]](partition.size)(Nil)

    /*
     * This is the main thing that is parallelized in the worker.
     */
    partition.zipWithIndex.foreach({
      case (vertex, i) =>

        val substeps = vertex.update(step - 1, incoming(vertex))
        val substep = substeps((step - 1) % substeps.size)

        ///////////////////////////////////////////////////////////
        // If we have a crunch step...
        ///////////////////////////////////////////////////////////
        if (substep.isInstanceOf[CrunchStep[Data]]) {
          val crunchStep = substep.asInstanceOf[CrunchStep[Data]]
          // assume every vertex has crunch step at this point
          if (vertex == partition(0)) {
            // compute aggregated value

            // The .par call here is not doing anything, as partition is already
            // a parallel collection. It does however change the resulting type.
            val vertexValues = partition.par.map(v => v.value)

            val crunchResult = vertexValues.reduceLeft(crunchStep.cruncher)
            crunch = Some(Crunch(crunchStep.cruncher, crunchResult))
          }
        } ///////////////////////////////////////////////////////////
        // Normal substep...
        ///////////////////////////////////////////////////////////
        else {
          //println("substep object for substep " + ((step - 1) % substeps.size) + ": " + substep)
          val outgoing = substep.stepfun()
          // set step field of outgoing messages to current step
          for (out <- outgoing) out.step = step

          // This is a concurrent non-locking way to collect all our messages...
          outgoingPerVertex(i) = outgoing

        }

        // only worker which manages the first vertex evaluates
        // the termination condition
        if (vertex == partition(0) && global.cond()) {
          println(this + ": sending Stop to " + parent)
          parent ! "Stop"
          self.stop()
        }
    })

    // Collect all messages produced in the parallel foreach...
    allOutgoing = outgoingPerVertex.flatMap(x => x).toList

    incoming = new HashMap[Vertex[Data], List[Message[Data]]]() {
      override def default(v: Vertex[Data]) = List()
    }

    if (crunch.isEmpty) {
      for (out <- allOutgoing) {
        if (out.dest.worker == self) { // mention in paper
          incoming(out.dest) = out :: incoming(out.dest)
        } else
          out.dest.worker ! out
      }
      parent ! "Done" // parent checks for "Stop" message first
    } else {
      parent ! crunch.get
    }
  }

  def receive = {
    case msg: Message[Data] =>
      queue += msg

    case "Next" => // TODO: make it a class
      superstep()

    case CrunchResult(res: Data) =>
      // deliver as incoming message to all vertices
      for (vertex <- partition) {
        val msg = Message[Data](null, vertex, res)
        msg.step = step
        queue += msg
      }
      // immediately start new superstep (explain in paper)
      superstep()

    case "Stop" =>
      //exit()
      //      println(self + ": we'd like to stop now")
      self.stop()
  }

}

/*
class Foreman(parent: Actor, var children: List[Actor]) extends Actor {

  def waitForRepliesFrom(children: List[Actor], response: Option[AnyRef]) {
    if (children.isEmpty) {
      parent ! response.get
    } else {
      react {
        case c: Crunch[d] => // have to wait for results of all children
          val cruncher = c.cruncher
          val crunchResult = c.crunchResult
//          println(this + ": received " + c + " from " + sender)

          if (response.isEmpty) {
            waitForRepliesFrom(children.tail, Some(Crunch(cruncher, crunchResult)))
          } else {
            val previousCrunchResult = response.get.asInstanceOf[Crunch[d]].crunchResult
            // aggregate previous result with new response
            val newResponse = cruncher(crunchResult, previousCrunchResult)
            waitForRepliesFrom(children.tail, Some(Crunch(cruncher, newResponse)))
          }
        case any: AnyRef =>
          waitForRepliesFrom(children.tail, Some(any))
      }
    }
  }

  def act() {
    loop {
      react {
        case "Stop" =>
          if (sender != parent) {
            //println(this + ": received Stop from child, forwarding to " + parent)
            // do not wait for other children
            // forward to parent and exit
            parent ! "Stop"
          }
          exit()

        case msg : AnyRef =>
          if (sender == parent) {
            //println(this + ": received " + msg + " from " + sender)
            for (child <- children) {
              child ! msg
            }
          } else { // received from child
            val otherChildren = children.filterNot((child: Actor) => child == sender)
            val response: Option[AnyRef] = msg match {
              case Crunch(_, _) => Some(msg)
              case otherwise => Some(msg)
            }
            // wait for a message from each child
            waitForRepliesFrom(otherChildren, response)
          }
      }
    }
  }
}
*/

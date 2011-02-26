package processing.parallel

import scala.collection.mutable.HashMap

import scala.actors.{Actor, TIMEOUT}
import Actor._

/*
 * @param graph      the graph for which the worker manages a partition of vertices
 * @param partition  the list of vertices that this worker manages
 */
class Worker[Data](parent: Actor, partition: List[Vertex[Data]], global: Graph[Data]) extends Actor {
  var numSubsteps = 0
  var step = 0

  var id = Graph.nextId

  var incoming = new HashMap[Vertex[Data], List[Message[Data]]]() {
    override def default(v: Vertex[Data]) = List()
  }

  def superstep() {
    // remove all application-level messages from mailbox
    var done = false
    while (!done) {
      receiveWithin(0) {
        case msg: Message[Data] if (msg.step == step) =>
          incoming(msg.dest) = msg :: incoming(msg.dest)
        case TIMEOUT =>
          done = true
      }
    }

    step += 1 // beginning of next superstep

    var allOutgoing: List[Message[Data]] = List()
    var crunch: Option[Crunch[Data]] = None

    // iterate over all vertices that this worker manages
    for (vertex <- partition) {
      // compute outgoing messages using application-level `update` method
      // and forward to parent
      // paper: obtain chain of closures again for new params!!
      val substeps = vertex.update(step - 1, incoming(vertex))
      //println("#substeps = " + substeps.size)
      val substep = substeps((step - 1) % substeps.size)

      if (substep.isInstanceOf[CrunchStep[Data]]) {
        val crunchStep = substep.asInstanceOf[CrunchStep[Data]]
        // assume every vertex has crunch step at this point
        if (vertex == partition(0)) {
          // compute aggregated value
          val vertexValues = partition.map(v => v.value)
          val crunchResult = vertexValues.reduceLeft(crunchStep.cruncher)
          crunch = Some(Crunch(crunchStep.cruncher, crunchResult))
        }
      } else {
        //println("substep object for substep " + ((step - 1) % substeps.size) + ": " + substep)
        val outgoing = substep.stepfun()
        // set step field of outgoing messages to current step
        for (out <- outgoing) out.step = step
        allOutgoing = allOutgoing ::: outgoing
      }

      // only worker which manages the first vertex evaluates
      // the termination condition
      //if (vertex == parent.vertices(0) && parent.cond())
      if (vertex == partition(0) && global.cond()) { // TODO: check!!
        //println(this + ": sending Stop to " + parent)
        parent ! "Stop"
        exit()
      }
    }
    incoming = new HashMap[Vertex[Data], List[Message[Data]]]() {
      override def default(v: Vertex[Data]) = List()
    }

    if (crunch.isEmpty) {
/*
      parent ! "Done" // parent checks for "Stop" message first
      react {
        case "Outgoing" => // message from parent: deliver outgoing messages!
*/
          for (out <- allOutgoing) {
            if (out.dest.worker == this) { // mention in paper
              incoming(out.dest) = out :: incoming(out.dest)
            } else
              out.dest.worker ! out
          }
          parent ! "Done" // parent checks for "Stop" message first
          //parent ! "DoneOutgoing"
//      }
    } else
      parent ! crunch.get
  }

  def act() {
/*    react {
      case "Init" =>
        for (v <- partition) { v.initialize() }
*/
        loop {
          react {
            case "Next" => // TODO: make it a class
              //println(this + ": received Next")
              superstep()

            case CrunchResult(res: Data) =>
              //println(this + ": received CrunchResult")
              // deliver as incoming message to all vertices
              for (vertex <- partition) {
                val msg = Message[Data](null, vertex, res)
                msg.step = step
                this ! msg
              }
            // immediately start new superstep (explain in paper)
            superstep()

            case "Stop" =>
              exit()
          }
        }
    //}
  } // def act()

}

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


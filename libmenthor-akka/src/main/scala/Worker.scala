package menthor.akka

import scala.collection.mutable.{HashMap, Queue}

import akka.actor.{Actor, ActorRef}
import akka.actor.Actor.actorOf

/*
 * @param graph      the graph for which the worker manages a partition of vertices
 * @param partition  the list of vertices that this worker manages
 */
class Worker[Data](parent: ActorRef, partition: List[Vertex[Data]], global: Graph[Data]) extends Actor {
  var numSubsteps = 0
  var step = 0

  var id = Graph.nextId

  var incoming = new HashMap[Vertex[Data], List[Message[Data]]]() {
    override def default(v: Vertex[Data]) = List()
  }

  val queue = new Queue[Message[Data]]
  val later = new Queue[Message[Data]]

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

    for (vertex <- partition) {
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
      if (vertex == partition(0) && global.cond()) {
        //println(this + ": sending Stop to " + parent)
        parent ! "Stop"
        //exit()
        println(self + ": we'd like to stop now")
        self.stop()
      }
    }
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
      println(self + ": we'd like to stop now")
      self.stop()
  }

}

package menthor.akka

import scala.collection.mutable.{ Map, HashMap }
import scala.collection.mutable.ListBuffer
import akka.actor
import actor.ActorRef
import actor.Actor.actorOf
import java.util.concurrent.CountDownLatch
import sun.reflect.generics.reflectiveObjects.NotImplementedException
import scala.collection.GenSeq
import scala.collection.parallel.mutable.ParArray

case class Message[Data](val source: Vertex[Data], val dest: Vertex[Data], val value: Data) {
  // TODO: for distribution need to change Vertex[Data] into vertex ID
  /* Superstep in which message was created.
   * TODO: step does not need to be visible in user code!
   */
  // step: the step in which the message was produced
  var step: Int = 0
}

/**
 * Each Substep has a substep function and a reference to the previous Substep
 * @param <Data>
 */
class Substep[Data](val stepfun: () => List[Message[Data]], val previous: Substep[Data]) {
  var next: Substep[Data] = null

  // TODO: implement thenUntil(cond)
  def then(block: => List[Message[Data]]): Substep[Data] = {
    next = new Substep(() => block, this)
    next
  }

  // TODO: merge crunch steps
  def crunch(fun: (Data, Data) => Data): Substep[Data] = {
    next = new CrunchStep(fun, this)
    next
  }

  def size: Int = {
    var currStep = firstSubstep
    var count = 1
    while (currStep.next != null) {
      count += 1
      currStep = currStep.next
    }
    count
  }

  private def firstSubstep = {
    // follow refs back to the first substep
    var currStep = this
    while (currStep.previous != null)
      currStep = currStep.previous
    currStep
  }

  def fromHere(num: Int): Substep[Data] = {
    if (num == 0)
      this
    else
      this.next.fromHere(num - 1)
  }

  def apply(index: Int): Substep[Data] = {
    val first = firstSubstep
    if (index == 0) {
      first
    } else {
      first.next.fromHere(index - 1)
    }
  }
}

class CrunchStep[Data](val cruncher: (Data, Data) => Data, previous: Substep[Data]) extends Substep[Data](null, previous)
case class Crunch[Data](val cruncher: (Data, Data) => Data, val crunchResult: Data)
case class CrunchResult[Data](res: Data)

/**
 * An abstract vertex in the graph which is to be processed by Menthor.
 * @param <Data>
 */
abstract class Vertex[Data](val label: String, initialValue: Data) {
  // TODO: subclasses should not be able to mutate the list!
  var neighbors: List[Vertex[Data]] = List()
  var value: Data = initialValue
  var graph: Graph[Data] = null

  // graph has to initialize `worker` upon partitioning the list of vertices
  // TODO: should not be accessible from subclasses!
  var worker: ActorRef = null

  implicit def mkSubstep(block: => List[Message[Data]]) =
    new Substep(() => block, null)

  // directed graph for now
  // TODO: should not be accessible from subclasses!
  def connectTo(v: Vertex[Data]) {
    neighbors = v :: neighbors
  }

  def initialize() {}

  def update(superstep: Int, incoming: List[Message[Data]]): Substep[Data]

  override def toString = "Vertex(" + label + ")"
}

object Graph {
  var count = 0
  def nextId = {
    count += 1
    count
  }
}

/** Message type to indicate to graph that it should start propagation */
case class StartPropagation(numIterations: Int, latch: CountDownLatch)

/**
 * The {GraphOperatingMode} defines how many local parallel workers
 * are to be created by the graph.
 * @author fgysin
 */
sealed case class GraphOperatingMode
/** Only one global worker, this is suitable for parallel collections */
case object SingleWorkerMode extends GraphOperatingMode
/** As many workers as there are processors available */
case object MultiWorkerMode extends GraphOperatingMode
/**
 * As many workers as specified by the user
 * FixedWorkerMode is mainly for testin/debugging/benchmarking purposes
 */
case class FixedWorkerMode(noOfWorkers: Int) extends GraphOperatingMode
/** As many workers as there are vertices */
case object IAmLegionMode extends GraphOperatingMode

/**
 * The main actor instance in a Menthor graph computation. The graph is the
 * parent of all graph workers.
 * @param <Data>
 */
class Graph[Data] extends actor.Actor {

  /**
   * If one would wish to turn off parallel collections make the following collection a List.
   */
  var vertices: GenSeq[Vertex[Data]] = ParArray()
  //  var vertices: GenSeq[Vertex[Data]] = List()
  var workers: List[ActorRef] = List()
  var allForemen: List[ActorRef] = List()
  var opmode: GraphOperatingMode = SingleWorkerMode
  var cond: () => Boolean = () => false
  var crunchResult: Option[Data] = None
  var shouldFinish = false
  var i = 1
  var numIterations = 0
  var numDone = 0
  var numTotal: Int = workers.size
  var cruncher: Option[(Data, Data) => Data] = None
  var workerResults: List[Data] = List()
  var numRecv = 0
  var parent: CountDownLatch = null

  /**
   * Add a vertex to this graph.
   * @param v Vertex to add.
   * @return
   */
  def addVertex(v: Vertex[Data]): Vertex[Data] = {
    v.graph = this
    vertices = v +: vertices
    v
  }

  /**
   * Set the operating mode for this graph defining the number of parallel (local) workers.
   * @param op
   */
  def setOpMode(op: GraphOperatingMode) = {
    this.opmode = op
  }

  /**
   * Create number of workers depending on the graph operating mode.
   * @param graphSize
   */
  def createWorkers() {
    val numProcs = Runtime.getRuntime().availableProcessors()
    println("Available processors: " + numProcs)

    opmode match {
      case SingleWorkerMode => {
        println("Creating a single worker. partition size = graph size = " + vertices.size)
        val worker = actorOf(new Worker(self, vertices, this))
        workers ::= worker
        for (v <- vertices)
          v.worker = worker
        worker.start()
      }
      case MultiWorkerMode => {
        println("Creating one worker per core. #workers = " + numProcs)
        var partitions: List[GenSeq[Vertex[Data]]] = partitionData(vertices, numProcs)
        for (partition <- partitions) {
          val worker = actorOf(new Worker(self, partition, this))
          workers ::= worker
          for (v <- partition)
            v.worker = worker
          worker.start()
        }
      }
      case FixedWorkerMode(fixedWorkerNo: Int) => {
        println("Creating fixed number of workers: " + fixedWorkerNo)
        var partitions: List[GenSeq[Vertex[Data]]] = partitionData(vertices, fixedWorkerNo)
        for (partition <- partitions) {
          val worker = actorOf(new Worker(self, partition, this))
          workers ::= worker
          for (v <- partition)
            v.worker = worker
          worker.start()
        }
      }
      case IAmLegionMode => {
        println("Creating one worker per vertex. #workers = graph size = " + vertices.size)
        for (v <- vertices) {
          val worker = actorOf(new Worker(self, List(v), this))
          workers ::= worker
          v.worker = worker
          worker.start()
        }
      }
      case _ => throw new IllegalArgumentException("Unknown graph operating mode.")
    }

    allForemen = workers
  }

  /**
   * This is creating partitions without relying on API that is not provided by GenSeq.
   * @param data General collection of graph vertices to partition.
   * @param n Number of partitions.
   * @return List of partitions.
   */
  def partitionData(data: GenSeq[Vertex[Data]], n: Int): List[GenSeq[Vertex[Data]]] = {
    val partitionSize: Int = if (data.size % n == 0) { data.size / n } else { (data.size / n) + 1 }
    var partitions: List[GenSeq[Vertex[Data]]] = List()

    var startOffset = 0
    var endOffset = partitionSize
    while (startOffset < vertices.size) {
      partitions = vertices.slice(startOffset, endOffset) :: partitions
      startOffset = endOffset
      endOffset = Math.min(endOffset + partitionSize, vertices.size)
    }

    partitions
  }

  /**
   * Start the next iteration.
   */
  def nextIter() {
    numRecv = 0

    if (shouldFinish) {
      // Stop all workers...
      for (w <- workers) {
        w ! "Stop"
      }
      // ...then stop yourself.
      self.exit()

    } else if ((numIterations == 0 || i <= numIterations) && !shouldFinish) {
      i += 1

      if (!crunchResult.isEmpty) {
        for (w <- workers) { // go to next superstep
          w ! CrunchResult(crunchResult.get)
        }
      } else {
        for (w <- workers) { // go to next superstep
          w ! "Next"
        }
      }

      numDone = 0
      numTotal = workers.size
      if (numTotal < 100) numTotal = 100

      cruncher = None
      workerResults = List()
    } else {

      parent.countDown()
    }
  }

  /* (non-Javadoc)
   * @see akka.actor.Actor#receive()
   */
  def receive = {
    case "Stop" =>
      println(self + ": we'd like to stop now")
      self.stop()

    case StartPropagation(numIters, from) =>
      // This is called on start...
      parent = from
      numIterations = numIters
      createWorkers

      become {
        case "Stop" => // stop iterating
          shouldFinish = true
          nextIter()

        case Crunch(fun: ((Data, Data) => Data), workerResult: Data) =>
          numRecv += 1
          if (cruncher.isEmpty)
            cruncher = Some(fun)
          workerResults ::= workerResult
          if (numRecv == workers.size) {
            crunchResult = Some(workerResults.reduceLeft(cruncher.get))
            nextIter()
          }

        case "Done" =>
          numRecv += 1
          numDone += 1

          if (numRecv == workers.size) {
            // are we inside a crunch step?
            if (!cruncher.isEmpty) {
              crunchResult = Some(workerResults.reduceLeft(cruncher.get))
            } else {
              crunchResult = None
            }
            nextIter()
          }
      }

      nextIter()
  }

  def iterate(numIterations: Int) {
    val l = new CountDownLatch(1)
    self ! StartPropagation(numIterations, l)
    l.await()
  }

  def iterateUntil(condition: => Boolean) {
    cond = () => condition
    val l = new CountDownLatch(1)
    self ! StartPropagation(0, l)
    l.await()
  }

  def terminate() {
    self ! "Stop"
  }
}

//////////////////////////////////////////////////////////////////////////////
// Tests
//////////////////////////////////////////////////////////////////////////////

object Test1 {
  var count = 0
  def nextcount: Int = {
    count += 1
    count
  }
}

class Test1Vertex extends Vertex[Double]("v" + Test1.nextcount, 0.0d) {
  def update(superstep: Int, incoming: List[Message[Double]]): Substep[Double] = {
    {
      value += 1
      List()
    }
  }
}

class Test2Vertex extends Vertex[Double]("v" + Test1.nextcount, 0.0d) {
  def update(superstep: Int, incoming: List[Message[Double]]): Substep[Double] = {
    {
      value += 1
      List()
    } crunch ((v1: Double, v2: Double) => v1 + v2) then {
      // result of crunch should be here as incoming message
      incoming match {
        case List(crunchResult) =>
          value = crunchResult.value
        case List() =>
        // do nothing
      }
      List()
    }
  }
}

object Test {

  def runTest1() {
    println("running test1...")
    var g: Graph[Double] = null
    val ga = actorOf({ g = new Graph[Double]; g })
    for (i <- 1 to 48) {
      g.addVertex(new Test1Vertex)
    }
    ga.start()
    g.iterate(1)
    g.synchronized {
      for (v <- g.vertices) {
        if (v.value < 1) throw new Exception
      }
    }
    g.terminate()
    println("test1 OK")
  }

  def runTest2() {
    println("running test2...")
    var g: Graph[Double] = null
    val ga = actorOf({ g = new Graph[Double]; g })
    for (i <- 1 to 10) {
      g.addVertex(new Test2Vertex)
    }
    ga.start()
    g.iterate(3)
    g.synchronized {
      for (v <- g.vertices) {
        if (v.value < 10) throw new Exception
      }
    }
    g.terminate()
    println("test2 OK")
  }

  def main(args: Array[String]) {
    runTest1()
    runTest2()
  }

}

package menthor.akka

import scala.collection.mutable.{Map, HashMap}
import scala.collection.mutable.ListBuffer

import akka.actor
import actor.ActorRef
import actor.Actor.actorOf

import java.util.concurrent.CountDownLatch

// step: the step in which the message was produced
// TODO: for distribution need to change Vertex[Data] into vertex ID
case class Message[Data](val source: Vertex[Data], val dest: Vertex[Data], val value: Data) {
  /* Superstep in which message was created.
   * TODO: step does not need to be visible in user code!
   */
  var step: Int = 0
}

// each Substep has a substep function and a reference to the previous Substep
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

// To point out in paper: it's not a DSL for expressing/constructing/transforming graphs,
// but rather a high-level API for graph /processing/.
abstract class Vertex[Data](val label: String, initialValue: Data) {
  // TODO: subclasses should not be able to mutate the list!
  var neighbors: List[Vertex[Data]] = List()

  var value: Data = initialValue

  var graph: Graph[Data] = null

  // graph has to initialize `worker` upon partitioning the list of vertices
  // TODO: should not be accessible from subclasses!
  var worker: ActorRef = null

  /*
   * { ...
   *   List()
   * } then {
   *   ...
   * }
   */
  implicit def mkSubstep(block: => List[Message[Data]]) =
    new Substep(() => block, null)

  // directed graph for now
  // TODO: should not be accessible from subclasses!
  def connectTo(v: Vertex[Data]) {
    neighbors = v :: neighbors
  }
  
  def initialize() { }

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

// Message type to indicate to graph that it should start propagation
case class StartPropagation(numIterations: Int, latch: CountDownLatch)

// TODO: maintain mapping from vertex to the worker which manages the vertex,
// do this using worker field of Vertex
class Graph[Data] extends actor.Actor {
  var vertices: List[Vertex[Data]] = List()
  var workers: List[ActorRef] = List()
  var allForemen: List[ActorRef] = List()

  var cond: () => Boolean = () => false

  //Debug.level = 3

  def addVertex(v: Vertex[Data]): Vertex[Data] = {
    v.graph = this
    vertices = v :: vertices
    v
  }

  def createWorkers(graphSize: Int) {
    val numProcs = Runtime.getRuntime().availableProcessors()

    if (!singleVertexGraph) {
      // Figure out correct partition size for vertex distribution...
      val partitionSize: Int = if (graphSize % numProcs == 0) { graphSize / numProcs } else { (graphSize / numProcs) + 1 }
      val partitions = vertices.grouped(partitionSize)
      println("Creating workers per partitions. Partition size = " + partitionSize)

      for (partition <- partitions) {
        val worker = actorOf(new Worker(self, partition, this))
        workers ::= worker
        for (v <- partition)
          v.worker = worker
        worker.start()
      }
    } else {
      println("Creating one worker per vertex.")
      for (v <- vertices) {
        val worker = actorOf(new Worker(self, List(v), this))
        workers ::= worker
        v.worker = worker
        worker.start()
      }
    }
    allForemen = workers
  }

/*
  def createWorkers() {
    val numChildren = 4
    val numWorkers = 48

    val topLevel = (for (i <- 1 to 3) yield {
      val f = new Foreman(this, List())
      workers = f :: workers
      f
    }).toList

    val listOfMidLevels = for (foreman <- topLevel) yield {
      val midLevel: List[Foreman] =
        (for (i <- 1 to numChildren) yield new Foreman(foreman, List())).toList
      foreman.children = midLevel
      midLevel
    }

    val midLevel = listOfMidLevels.flatten
    allForemen = topLevel ::: midLevel

    // partition set of vertices into numWorkers partitions
    val sizeOfMostPartitions = (vertices.size.toDouble / numWorkers.toDouble).floor.toInt
    val tooManyPartitions = vertices.grouped(sizeOfMostPartitions).toList
    var additionalPartitions = tooManyPartitions.drop(numWorkers)
    val actualPartitions = tooManyPartitions.take(numWorkers)

    val partitions = for (part <- actualPartitions) yield {
      if (additionalPartitions.isEmpty)
        part
      else {
        val additionalPart = additionalPartitions.head
        additionalPartitions = additionalPartitions.tail
        part ::: additionalPart
      }
    }

/*
    val partitionSize = {
      val tmp = (vertices.size.toDouble / numWorkers.toDouble).ceil.toInt
      if (tmp == 0) 1 else tmp
    }
    println("partition size: " + partitionSize)
    val partitions = vertices.grouped(partitionSize).toList
*/
    //println("#partitions: " + partitions.size)

    var partition: List[Vertex[Data]] = List()
    for (i <- 0 until 12; j <- 0 until 4) {
      partition = partitions(i * 4 + j)
      val worker = new Worker(midLevel(i), partition, this)
      midLevel(i).children = worker :: midLevel(i).children
      allForemen ::= worker

      for (vertex <- partition) {
        vertex.worker = worker
      }
      worker.start()
    }

    for (f <- midLevel) { f.start() }
    for (f <- topLevel) { f.start() }
  }
*/

  var crunchResult: Option[Data] = None
  var shouldFinish = false
  var i = 1
  var numIterations = 0

  var numDone = 0
  var numTotal: Int = workers.size

  var cruncher: Option[(Data, Data) => Data] = None
  var workerResults: List[Data] = List()

  def nextIter() {
    numRecv = 0
    if ((numIterations == 0 || i <= numIterations) && !shouldFinish) {
      i += 1
      // 0 superstep: i == 2
      // 1 superstep: i == 3

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
/*
      for (foreman <- allForemen) {
        foreman ! "Stop"
      }
*/
      parent.countDown()
    }
  }

  var numRecv = 0
  var parent: CountDownLatch = null

  def receive = {
    case "Stop" =>
      //exit()
      println(self + ": we'd like to stop now")
      self.stop()

    case StartPropagation(numIters, from) =>
      parent = from
      numIterations = numIters
      createWorkers(vertices.size)

      become {
        case "Stop" => // stop iterating
          //println("should stop now (received from " + sender + ")")
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
          //if ((numDone % (numTotal / 20)) == 0)
          //  print("#")

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
      //println(".")

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

      //, (res: Double, vertices: (Vertex, Vertex)) => {
      // (T, (Vertex, Vertex)) => List[Message[Data]]

      // (Vertex, Vertex) => (T, (Vertex, Vertex))
      //(v1.value + v2.value, (v1, v2))

class Test2Vertex extends Vertex[Double]("v" + Test1.nextcount, 0.0d) {
  def update(superstep: Int, incoming: List[Message[Double]]): Substep[Double] = {
    {
      value += 1
      List()
    } crunch((v1: Double, v2: Double) => v1 + v2) then {
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

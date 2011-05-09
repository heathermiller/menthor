package menthor.akka

import java.util.concurrent.CountDownLatch

import akka.actor
import actor.ActorRef
import actor.Actor.actorOf

object Graph {
  var count = 0
  def nextId = {
    count += 1
    count
  }
}

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

    if (graphSize % numProcs == 0) {
      val partitionSize: Int = graphSize / numProcs
      val partitions = vertices.grouped(partitionSize)
      for (partition <- partitions) {
        val worker = actorOf(new Worker(self, partition, this))
        workers ::= worker
        for (v <- partition)
          v.worker = worker
        worker.start()
      }
    } else {
      // create one worker per vertex
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

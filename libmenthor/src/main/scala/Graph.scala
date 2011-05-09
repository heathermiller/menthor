package processing.parallel

import scala.collection.mutable.{Map, HashMap}
import scala.collection.mutable.ListBuffer

import scala.actors.{Actor, TIMEOUT, OutputChannel, Debug}
import Actor._

object Graph {
  var count = 0
  def nextId = synchronized {
    count += 1
    count
  }
}

// TODO: maintain mapping from vertex to the worker which manages the vertex,
// do this using worker field of Vertex
class Graph[Data] extends Actor {
  var vertices: List[Vertex[Data]] = List()
  var workers: List[Actor] = List()
  var allForemen: List[Actor] = List()

  var cond: () => Boolean = () => false

  //Debug.level = 3

  def addVertex(v: Vertex[Data]): Vertex[Data] = {
    v.graph = this
    vertices = v :: vertices
    v
  }

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

  def act() {
    loop {
      receive {
        case "Stop" =>
          exit()

        case StartPropagation(numIterations) =>
          val parent = sender
          createWorkers()
 
          var crunchResult: Option[Data] = None
          var shouldFinish = false
          var i = 1
          while ((numIterations == 0 || i <= numIterations) && !shouldFinish) {
            i += 1
            
            if (!crunchResult.isEmpty)
              for (w <- workers) { // go to next superstep
                w ! CrunchResult(crunchResult.get)
              }
            else
              for (w <- workers) { // go to next superstep
                w ! "Next"
              }

            var numDone = 0
            var numTotal: Int = workers.size
            if (numTotal < 100) numTotal = 100

            var cruncher: Option[(Data, Data) => Data] = None
            var workerResults: List[Data] = List()

            for (w <- workers) {
              receive {
                case "Stop" => // stop iterating
                  //println("should stop now (received from " + sender + ")")
                  shouldFinish = true

                case Crunch(fun: ((Data, Data) => Data), workerResult: Data) =>
                  if (cruncher.isEmpty)
                    cruncher = Some(fun)
                  workerResults ::= workerResult

                case "Done" =>
                  numDone += 1
                  //if ((numDone % (numTotal / 20)) == 0)
                  //  print("#")
              }
            }
            //println(".")

            if (!shouldFinish) {
              // are we inside a crunch step?
              if (!cruncher.isEmpty)
                crunchResult = Some(workerResults.reduceLeft(cruncher.get))
              else
                crunchResult = None

                //println(this + ": sending Outgoing to " + workers)
/*
                for (w <- workers) {
                  w ! "Outgoing"
                }
                for (w <- workers) {
                  receive {
                    case "DoneOutgoing" =>
                      //println(this + ": received DoneOutgoing")
                  }
                }
*/
            }
          }

          for (foreman <- allForemen) {
            foreman ! "Stop"
          }
          parent ! "Done"
      }
    }
  }

  def iterate(numIterations: Int) {
    this !? StartPropagation(numIterations)
  }

  def iterateUntil(condition: => Boolean) {
    cond = () => condition
    this !? StartPropagation(0)
  }

  def terminate() {
    this ! "Stop"
  }
}

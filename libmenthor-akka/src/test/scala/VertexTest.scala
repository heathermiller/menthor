package menthor.akka

import org.scalatest.FunSuite
import akka.actor.Actor.actorOf

class VertexTest extends FunSuite {

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

  test("test1") {
    var g: Graph[Double] = null
    val ga = actorOf({ g = new Graph[Double]; g })
    for (i <- 1 to 48) {
      g.addVertex(new Test1Vertex)
    }
    ga.start()
    g.iterate(1)
    g.synchronized {
      for (v <- g.vertices) {
        assert(v.value >= 1)
      }
    }
    g.terminate()
  }

  test("test2") {
    var g: Graph[Double] = null
    val ga = actorOf({ g = new Graph[Double]; g })
    for (i <- 1 to 48) {
      g.addVertex(new Test2Vertex)
    }
    ga.start()
    g.iterate(3)
    g.synchronized {
      for (v <- g.vertices) {
        assert(v.value >= 10)
      }
    }
    g.terminate()
  }

}


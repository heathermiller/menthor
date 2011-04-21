package processing.parallel

import org.scalatest.FunSuite

class VertexTest extends FunSuite {

  object Test1 {
    var count = 0
    def nextcount: Int = {
      count += 1
      count
    }
  }

  class Test1Vertex extends Vertex[Double]("v" + Test1.nextcount, 0.0d) {
    def update(): Substep[Double] = {
      {
        value += 1
        List()
      }
    }
  }

  class Test2Vertex extends Vertex[Double]("v" + Test1.nextcount, 0.0d) {
    def update(): Substep[Double] = {
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
    val g = new Graph[Double]
    for (i <- 1 to 48) {
      g.addVertex(new Test1Vertex)
    }
    g.start()
    g.iterate(1)
    g.synchronized {
      for (v <- g.vertices) {
        assert(v.value >= 1)
      }
    }
    g.terminate()
  }

  test("test2") {
    val g = new Graph[Double]
    for (i <- 1 to 48) {
      g.addVertex(new Test2Vertex)
    }
    g.start()
    g.iterate(3)
    g.synchronized {
      for (v <- g.vertices) {
        assert(v.value >= 10)
      }
    }
    g.terminate()
  }

}


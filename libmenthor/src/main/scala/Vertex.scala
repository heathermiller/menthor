package processing.parallel

import scala.actors.Actor

// To point out in paper: it's not a DSL for expressing/constructing/transforming graphs,
// but rather a high-level API for graph /processing/.
abstract class Vertex[Data](val label: String, initialValue: Data) {
  // TODO: subclasses should not be able to mutate the list!
  var neighbors: List[Vertex[Data]] = List()

  var value: Data = initialValue

  var graph: Graph[Data] = null

  // graph has to initialize `worker` upon partitioning the list of vertices
  // TODO: should not be accessible from subclasses!
  var worker: Actor = null

  var superstep: Int = 0
  var incoming: List[Message[Data]] = List()

  private[parallel] var currentStep: Substep[Data] =
    // construct pipeline of substeps
    update().firstSubstep

  private[parallel] def moveToNextStep() {
    currentStep =
      if (currentStep.next == null) currentStep.firstSubstep
      else currentStep.next
  }

  // returns either Option[Crunch[Data]] or List[Message[Data]]
  // assume !substep.isInstanceOf[CrunchStep[Data]]
  // (checked in Worker.superstep)
/*
  private[parallel] def executeNextStep(): Option[List[Message[Data]]] = {
    //println("#substeps = " + substeps.size)
    //val substep = substeps((step - 1) % substeps.size)
    val substep = currentStep

    //println("substep object for substep " + ((step - 1) % substeps.size) + ": " + substep)
    if (!substep.cond.isEmpty) {
      // execute step only if condition is false
      val cond = substep.cond.get
      if (!cond()) {
        Some(substep.stepfun()) // execute step function
      } else None


 {
        moveToNextStep()
        executeNextStep()
      }
    }
  }
*/

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

  def update(): Substep[Data]

  override def toString = "Vertex(" + label + ")"
}

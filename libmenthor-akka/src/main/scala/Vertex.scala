package menthor.akka.processing

abstract class Vertex[Data] {
  private[processing] var currentStep: Step[Data] =
    update().first

  private[processing] def moveToNextStep() {
    currentStep = currentStep.next getOrElse currentStep.first
  }

  private implicit def mkSubstep(block: => List[Message[Data]]) =
    new Substep(() => block)

  protected def update(): Step[Data]
}

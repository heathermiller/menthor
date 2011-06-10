package menthor.processing

trait Vertex[Data] {
  final implicit val ref: VertexRef = new VertexRef

  private[processing] val worker: Worker[Data] = Worker.vertexCreator.get match {
    case Some(wrkr) => wrkr.asInstanceOf[Worker[Data]]
    case None => throw new IllegalStateException("Not inside a worker")
  }

  protected var data = initialValue

  def connectTo(successor: VertexRef): Unit

  def neighbors: Iterable[VertexRef]

  final def numVertices: Int = worker.totalNumVertices

  final def value: Data = data

  def initialValue: Data

  def initialize() { }

  implicit final def superstep: Superstep = worker.superstep

  protected final def incoming: List[Message[Data]] = worker.incoming(ref.uuid)

  private[processing] var currentStep: Step[Data] =
    update().first

  private[processing] def moveToNextStep() {
    currentStep = currentStep.next getOrElse currentStep.first
  }

  protected implicit final def mkSubstep(block: => List[Message[Data]]): Step[Data] =
    new Substep(block _)

  protected final def until(cond: => Boolean)(block: => List[Message[Data]]): Step[Data] =
    new Substep(block _, None, Some(cond _))

  protected final def crunch(fun: (Data, Data) => Data): Step[Data] =
    new CrunchStep(fun)

  protected def update(): Step[Data]

  protected final def voteToHalt(): List[Message[Data]] = {
    worker.voteToHalt(ref.uuid)
    Nil
  }
}

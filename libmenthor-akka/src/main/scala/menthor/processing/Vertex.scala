package menthor.processing

trait Vertex[Data] {
  final implicit val ref: VertexRef = new VertexRef

  private[processing] val worker: Worker[Data] = Worker.vertexCreator.get match {
    case Some(wrkr) => wrkr.asInstanceOf[Worker[Data]]
    case None => throw new IllegalStateException("Not inside a worker")
  }

  private var data = initialValue

  def connectTo(successor: VertexRef): Unit

  def neighbors: Iterable[VertexRef]

  final def numVertices: Int = worker.totalNumVertices

  final def value: Data = data

  protected final def value_=(x: Data) { data = x }

  def initialValue: Data

  def initialize() { }

  implicit final def superstep: Superstep = worker.superstep

  protected final def incoming: List[Message[Data]] = worker.incoming(ref.uuid)

  private[processing] var currentStep: Step[Data] = update.first

  private[processing] def moveToNextStep() {
    currentStep = currentStep.next getOrElse currentStep.first
  }

  final def process(block: => List[Message[Data]]) = new Substep(block _)

  protected final def until(cond: => Boolean)(block: => List[Message[Data]]): Step[Data] = new Substep(block _, None, Some(cond _))

  protected final def crunch(fun: (Data, Data) => Data): Step[Data] = new CrunchStep(fun)

  protected def update: Step[Data]

  protected final def voteToHalt: List[Message[Data]] = {
    worker.voteToHalt(ref.uuid)
    Nil
  }
}

trait SimpleVertex[Data] extends Vertex[Data] {
  private var _neighbors: List[VertexRef] = Nil
  def neighbors = _neighbors
  def connectTo(successor: VertexRef) {
    _neighbors = successor :: _neighbors
  }
}

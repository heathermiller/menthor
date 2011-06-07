package menthor.akka.processing

class Step[Data](
  val previous: Option[Step[Data]] = None,
  val cond: Option[() => Boolean] = None
) {
  val first: Step[Data] = previous getOrElse this
  var next: Option[Step[Data]] = None

  def then(block: () => List[Message[Data]]): Step[Data] = {
    next = Some(new Substep(block, Some(this)))
    next.get
  }

  def crunch(fun: (Data, Data) => Data): Step[Data] = {
    next = Some(new CrunchStep(fun, Some(this)))
    next.get
  }

  def thenUntil(cond: () => Boolean)(block: () => List[Message[Data]]): Step[Data] = {
    next = Some(new Substep(block, Some(this), Some(cond)))
    next.get
  }

  def size: Int = {
    var current = first
    var count = 1
    while (current.next.isDefined) {
      count += 1
      current = current.next.get
    }
    count
  }
}

class Substep[Data](
  val stepfun: () => List[Message[Data]],
  previous: Option[Step[Data]] = None,
  cond: Option[() => Boolean] = None
) extends Step[Data](previous, cond)

class CrunchStep[Data](
  val cruncher: (Data, Data) => Data,
  previous: Option[Step[Data]] = None
) extends Step[Data](previous)

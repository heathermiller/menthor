package menthor.processing

class Step[Data](
  val previous: Option[Step[Data]] = None,
  val cond: Option[() => Boolean] = None
) {
  def this(previous: Step[Data], cond: () => Boolean) = this(Some(previous), Some(cond))
  def this(previous: Step[Data]) = this(Some(previous))
  def this(cond: () => Boolean) = this(cond = Some(cond))

  val first: Step[Data] = if (previous.isDefined) previous.get.first else this
  var next: Option[Step[Data]] = None

  def then(block: => List[Message[Data]]): Step[Data] = {
    next = Some(new Substep(() => block, this))
    next.get
  }

  def crunch(fun: (Data, Data) => Data): Step[Data] = {
    next = Some(new CrunchStep(fun, this))
    next.get
  }

  def thenUntil(cond: => Boolean)(block: => List[Message[Data]]): Step[Data] = {
    next = Some(new Substep(() => block, this, () => cond))
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
) extends Step[Data](previous, cond) {
  def this(stepfun: () => List[Message[Data]], previous: Step[Data], cond: () => Boolean) = this(stepfun, Some(previous), Some(cond))
  def this(stepfun: () => List[Message[Data]], previous: Step[Data]) = this(stepfun, Some(previous))
  def this(stepfun: () => List[Message[Data]], cond: () => Boolean) = this(stepfun, cond = Some(cond))
}

class CrunchStep[Data](
  val cruncher: (Data, Data) => Data,
  previous: Option[Step[Data]] = None
) extends Step[Data](previous) {
  def this(cruncher: (Data, Data) => Data, previous: Step[Data]) = this(cruncher, Some(previous))
}

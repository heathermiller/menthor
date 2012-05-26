package processing.parallel

// each Substep has a substep function and a reference to the previous Substep
class Substep[Data](val stepfun: () => List[Message[Data]], val previous: Substep[Data]) {
  var next: Substep[Data] = null

  // TODO: implement thenUntil(cond)
  def then(block: => List[Message[Data]]): Substep[Data] = {
    next = new Substep(() => block, this)
    next
  }

  // TODO: merge crunch steps
  def crunch(fun: (Data, Data) => Data): Substep[Data] = {
    next = new CrunchStep(fun, this)
    next
  }
  
  // PG
  def crunchToOne(fun: (Data, Data) => Data): Substep[Data] = {
    // OK !
    next = new CrunchToOneStep(fun, this)
    next
  }

  def size: Int = {
    var currStep = firstSubstep
    var count = 1
    while (currStep.next != null) {
      count += 1
      currStep = currStep.next
    }
    count
  }

  private def firstSubstep = {
    // follow refs back to the first substep
    var currStep = this
    while (currStep.previous != null)
      currStep = currStep.previous
    currStep
  }

  def fromHere(num: Int): Substep[Data] = {
    if (num == 0)
      this
    else
      this.next.fromHere(num - 1)
  }

  def apply(index: Int): Substep[Data] = {
    val first = firstSubstep
    if (index == 0) {
      first
    } else {
      first.next.fromHere(index - 1)
    }
  }
}

class CrunchStep[Data](val cruncher: (Data, Data) => Data, previous: Substep[Data]) extends Substep[Data](null, previous)

// Special step that indicates that only one vertex will get the result
class CrunchToOneStep[Data](cruncher: (Data, Data) => Data, previous: Substep[Data]) extends CrunchStep[Data](cruncher, previous)

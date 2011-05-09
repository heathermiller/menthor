package menthor.akka

/*
class Foreman(parent: Actor, var children: List[Actor]) extends Actor {

  def waitForRepliesFrom(children: List[Actor], response: Option[AnyRef]) {
    if (children.isEmpty) {
      parent ! response.get
    } else {
      react {
        case c: Crunch[d] => // have to wait for results of all children
          val cruncher = c.cruncher
          val crunchResult = c.crunchResult
//          println(this + ": received " + c + " from " + sender)

          if (response.isEmpty) {
            waitForRepliesFrom(children.tail, Some(Crunch(cruncher, crunchResult)))
          } else {
            val previousCrunchResult = response.get.asInstanceOf[Crunch[d]].crunchResult
            // aggregate previous result with new response
            val newResponse = cruncher(crunchResult, previousCrunchResult)
            waitForRepliesFrom(children.tail, Some(Crunch(cruncher, newResponse)))
          }
        case any: AnyRef =>
          waitForRepliesFrom(children.tail, Some(any))
      }
    }
  }

  def act() {
    loop {
      react {
        case "Stop" =>
          if (sender != parent) {
            //println(this + ": received Stop from child, forwarding to " + parent)
            // do not wait for other children
            // forward to parent and exit
            parent ! "Stop"
          }
          exit()

        case msg : AnyRef =>
          if (sender == parent) {
            //println(this + ": received " + msg + " from " + sender)
            for (child <- children) {
              child ! msg
            }
          } else { // received from child
            val otherChildren = children.filterNot((child: Actor) => child == sender)
            val response: Option[AnyRef] = msg match {
              case Crunch(_, _) => Some(msg)
              case otherwise => Some(msg)
            }
            // wait for a message from each child
            waitForRepliesFrom(otherChildren, response)
          }
      }
    }
  }
}
*/

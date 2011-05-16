package menthor.akka

import org.scalatest.FunSuite
import akka.actor.{Actor, ActorRef}
import Actor._

class ClusterServiceSuite extends FunSuite {
  test("create and contact foreman") {
    val clusterService = remote.actorFor("menthor-cluster-service", "localhost", 2552)
    clusterService !! CreateForeman match {
      case Some(foremanUuid: String) => contact(foremanUuid)
      case None => fail("timeout when contacting cluster service")
      case _ => fail("unknown response when creating foreman")
    }
  }

  def contact(foremanUuid: String) = {
    val foreman = remote.actorFor("uuid:" + foremanUuid, "localhost", 2552)
    foreman !! "hello" match {
      case Some("world") =>
      case None => fail("timeout when contacting foreman")
      case _ => fail("unknown response from foreman")
    }
  }
}


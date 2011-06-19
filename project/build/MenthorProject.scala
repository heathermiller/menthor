import sbt._

class MenthorProject(info: ProjectInfo) extends ParentProject(info) {

  lazy val libmenthor = project("libmenthor", "libmenthor",
    new DefaultProject(_) with ScalatestDep)

  lazy val libmenthor_akka = project("libmenthor-akka", "libmenthor-akka",
    new DefaultProject(_) with AkkaRemoteDep with ScalatestDep {
      override def compileOptions = super.compileOptions ++ Seq(Unchecked)
    } )

  lazy val examples = project("examples", "examples", new Examples(_))

  class Examples(info: ProjectInfo) extends ParentProject(info) {
    lazy val pagerank = project("pagerank", "pagerank", libmenthor)

    lazy val pagerank_akka = project("pagerank-akka", "pagerank-akka",
      libmenthor_akka)

    lazy val pagerank_akka_cs = project("pagerank-akka-cs", "pagerank-akka-cs",
      pagerank_akka)

    lazy val sssp = project("sssp", "sssp", libmenthor)

    lazy val clustering = project("clustering", "hierarchical-clustering",
      libmenthor)
  }

  trait AkkaRemoteDep extends AkkaProject {
    val akkaRemote = akkaModule("remote")
    }

  trait ScalatestDep {
    val scalatest = "org.scalatest" % "scalatest_2.9.0" % "1.6.1" % "test"
  }
}

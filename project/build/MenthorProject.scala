import sbt._

class MenthorProject(info: ProjectInfo) extends ParentProject(info) {

  lazy val libmenthor = project("libmenthor", "Menthor Library",
    new DefaultProject(_) with ScalatestDependency)

  lazy val libmenthor_akka = project("libmenthor-akka", "Menthor Library with"
    + " Akka", new DefaultProject(_) with AkkaProject)

  lazy val examples = project("examples", "Menthor Examples", new Examples(_))

  class Examples(info: ProjectInfo) extends ParentProject(info) {
    lazy val pagerank = project("pagerank", "Wikipedia Pagerank", libmenthor)

    lazy val pagerank_akka = project("pagerank-akka", "Wikipedia Pagerank with"
      + " Akka", libmenthor_akka)

    lazy val sssp = project("sssp", "Single Source Shortest Paths", libmenthor)

    lazy val clustering = project("clustering", "Hierarchical Clustering",
    libmenthor)
  }

  trait ScalatestDependency {
    lazy val scalatest = "org.scalatest" % "scalatest" % "1.3" % "test"
  }
}

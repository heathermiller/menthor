import sbt._

class MenthorProject(info: ProjectInfo) extends DefaultProject(info) with
    AkkaProject {
  val scalatest = "org.scalatest" % "scalatest" % "1.3" % "test"
}

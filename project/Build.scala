import sbt._
import Keys._

object BuildSettings {
  val buildOrganization = "epfl"
  val buildVersion = "0.1"
  val buildScalaVersion = "2.9.0-1"

  val buildSettings = Defaults.defaultSettings ++ Seq (
    organization := buildOrganization,
    version := buildVersion,
    scalaVersion := buildScalaVersion
  )
}

object Resolvers {
  val akkarepo = "Akka Repository" at "http://akka.io/repository"
  val guiceyfruitrepo = "GuiceyFruit Release Repository" at "http://guiceyfruit.googlecode.com/svn/repo/releases/"

  val akkaResolvers = Seq (akkarepo, guiceyfruitrepo)
}

object Dependencies {
  val akkaVer = "1.1.2"

  val akkaactor = "se.scalablesolutions.akka" % "akka-actor" % akkaVer
  val akkaremote = "se.scalablesolutions.akka" % "akka-remote" % akkaVer
  val scalatest = "org.scalatest" % "scalatest_2.9.0" % "1.6.1" % "test"
}

object MenthorBuild extends Build {
  import Resolvers._
  import Dependencies._
  import BuildSettings._

  val commonDeps = Seq (
    scalatest
  )

  val akkaDeps = Seq (
    akkaactor,
    akkaremote
  )

  lazy val libmenthor = Project (
    "libmenthor",
    file("libmenthor"),
    settings = buildSettings ++ Seq (libraryDependencies := commonDeps)
  )

  lazy val pagerank = Project (
    "pagerank",
    file("examples/pagerank"),
    settings = buildSettings
  ) dependsOn (libmenthor)

  lazy val sssp = Project (
    "sssp",
    file("examples/sssp"),
    settings = buildSettings
  ) dependsOn (libmenthor)

  lazy val clustering = Project (
    "hierarchical-clustering",
    file("examples/clustering"),
    settings = buildSettings
  ) dependsOn (libmenthor)

  lazy val libmenthor_akka = Project (
    "libmenthor-akka",
    file("libmenthor-akka"),
    settings = buildSettings ++ Seq (
      resolvers := akkaResolvers,
      libraryDependencies := akkaDeps ++ commonDeps
    )
  )

  lazy val pagerank_akka = Project (
    "pagerank-akka",
    file("examples/pagerank-akka"),
    settings = buildSettings ++ Seq (
      resolvers := akkaResolvers,
      libraryDependencies := akkaDeps
    )
  ) dependsOn (libmenthor_akka)
}

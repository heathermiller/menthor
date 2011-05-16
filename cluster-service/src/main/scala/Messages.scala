package menthor.akka

sealed abstract class ClusterMessage

case object CreateForeman extends ClusterMessage


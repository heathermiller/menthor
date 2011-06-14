package pagerank

import akka.actor.Actor.actorOf
import menthor.processing.GraphMaster

class PageRankOptions(args: Array[String]) {
  val links = args(0)
  val titles = args(1)
  val pages = args(2).toInt
  val iterations = if (args.size > 2) args(3).toInt else 0
}

object PageRank extends App {
  val opts = new PageRankOptions(args)
  val dataIO = new PageRankDataIO(opts)
  val master = actorOf(new GraphMaster(dataIO)).start()
}

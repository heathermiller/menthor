package pagerank

import akka.actor.Actor.actorOf
import menthor.processing.GraphMaster
import java.io.File

class PageRankOptions(args: Array[String]) extends Serializable {
  val links = args(0)
  val titles = args(1)
  val resultsDir = new File(args(2))
  val pages = args(3).toInt
  val iterations = if (args.size > 4) args(4).toInt else 0
}

object PageRank extends App {
  val opts = new PageRankOptions(args)
  val dataIO = new PageRankDataIOMaster(opts)
  val master = actorOf(new GraphMaster(dataIO)).start()
}

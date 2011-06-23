package menthor.config

import io.Source

class Config {
  val localWorkers: Int = {
    val workerCountSysProp = sys.props.get("menthor.workers").map(_.toInt)
    workerCountSysProp.filter(_ != 0) getOrElse Runtime.getRuntime.availableProcessors
  }

  val configuration: List[(String, Option[Int], Option[(WorkerModifier.Value, Int)])] = {
    try {
      if (sys.props.getOrElse("menthor.conf", "") != "") {
        val source = Source.fromFile(sys.props("menthor.conf"))
        ConfigParser.parseAll(ConfigParser.config, source.bufferedReader).get
      } else if (getClass.getClassLoader.getResource("menthor.conf") ne null) {
        val source = Source.fromURL(getClass.getClassLoader.getResource("menthor.conf"))
        ConfigParser.parseAll(ConfigParser.config, source.bufferedReader).get
      } else {
        Nil
      }
    } catch {
      case e => failure(e.getMessage)
    }
  }

  def failure(msg: String) = {
    System.err.println("Couldn't parse configuration file: " + msg)
    sys.exit(-1)
    Nil
  }
}

package menthor.config

import sys.SystemProperties
import io.Source

class Config {
  val sysProp = new SystemProperties

  val localWorkers: Int = {
    val workerCountSysProp = sysProp.get("menthor.workers").map(_.toInt)
    workerCountSysProp.filter(_ != 0) getOrElse Runtime.getRuntime.availableProcessors
  }

  val configuration: List[(String, Option[Int], Option[(WorkerModifier.Value, Int)])] = {
    try {
      if (sysProp contains "menthor.conf") {
        val source = Source.fromFile(sysProp("menthor.conf"))
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

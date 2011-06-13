package menthor.config

import sys.SystemProperties
import java.io.{File, FileInputStream, InputStreamReader, BufferedReader}

class Config {
  val sysProp = new SystemProperties

  val localWorkers: Int = {
    val workerCountSysProp = sysProp.get("menthor.workers").map(_.toInt)
    workerCountSysProp.filter(_ != 0) getOrElse Runtime.getRuntime.availableProcessors
  }

  val configuration: List[(String, Option[Int], Option[(WorkerModifier.Value, Int)])] = {
    val filename = sysProp.getOrElse("menthor.conf", "menthor.conf")
    val file = new File(filename)

    try {
      if (file.canRead) {
        val reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"))
        val parser = new ConfigParser
        parser.parseAll(parser.config, reader) match {
          case parser.Success(result, _) => result
          case x @ parser.NoSuccess(_, _) => failure(x.toString)
        }
      } else if (file.exists) {
        failure("cannot read file: " + file.getPath)
      } else if (sysProp.get("menthor.conf") isEmpty) {
        Nil
      } else {
        failure("file " + filename + " does not exist")
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

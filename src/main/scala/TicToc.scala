package benchmark

/**
 * TicToc allows for easy benchmarking of different parts in source code.
 * Just inherit the TicToc trait, then start a time measurement with tic,
 * and stop it using toc.
 * As of now tic-tocs can be nested but can not overlap, as the underlying
 * data structure is a stack: a toc always terminates the last tic.
 * The reason for this decision was that the TicToc trait and its methods
 * should be as lightweight as possible in order not to falsify the measured
 * run times.
 * @author fgysin
 */
trait TicToc {
  import scala.collection.mutable.LinkedList
  import scala.collection.mutable.Stack
  import java.io._

  var timeCounter: Int = 0
  var tics: Stack[Long] = Stack()
  var times: LinkedList[(String, Long)] = LinkedList()

  /**
   * Starts a time measurement.
   */
  def tic() {
    tics = tics.push(System.currentTimeMillis())
  }

  /**
   * Stops the last started time measurement.
   * @param descr a description of the last measurement period
   */
  def toc(descr: String) {
    var t = System.currentTimeMillis() - tics.pop()
    if (null != descr && !"".equals(descr))
      times = times :+ (descr, t)
    else {
      times = times :+ ("time" + timeCounter, t)
      timeCounter += 1
    }
  }

  /**
   * Write the logged times to a file. There is some formatting sugar contained
   * that checks if a file with this name already exists. If yes, the run times
   * are added at the bottom of the existing file (this is useful if you repeat
   * the same measurement multiple times), else the file is created.
   * @param path The path of the file to write the times log.
   */
  def writeTimesLog(path: String) {
    var linesToPrint = outLines
    try {
      // Check if the file was alreay created, if yes only 
      // append the timings not the timing information.
      val lr = new LineNumberReader(new FileReader(path))
      val read = lr.readLine()
      if (null != read && read.startsWith("Timings")) {
        linesToPrint = linesToPrint.drop(2)
      }
      lr.close()
    } catch {
      case _ =>
    }
    // Write the timings and or timing information.
    val fw = new FileWriter(path, true);
    linesToPrint.foreach { l => fw.write(l + "\n"); }
    fw.close()
  }

  /**
   * Print the logged times.
   */
  def printTimesLog() {
    outLines.foreach { l => println(l); }
  }

  /**
   * String output of the current times and their respective descriptions.
   * @return
   */
  def outLines(): List[String] = {
    var list = List[String]()
    list ::= ("Timings of " + this.getClass().toString())
    list ::= "description:" + times.map(x => "\t" + x._1).reduce(_ + _)
    list ::= "times(ms):" + times.map(x => "\t" + x._2).reduce(_ + _)
    list.reverse
  }

}

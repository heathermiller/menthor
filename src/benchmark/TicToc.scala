package benchmark

/**
 * TicToc allows for easy benchmarking of different parts in source code.
 * Just inherit the TicToc trait, then start a time measurement with tic,
 * and stop it using toc.
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
   * Write the logged times to a file.
   * @param path
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
    linesToPrint.foreach { l => fw.write(l+"\n"); }
    fw.close()
  }

  /**
   * Print the logged times.
   */
  def printTimesLog() {
    outLines.foreach { l => println(l); }
  }

  def outLines(): List[String] = {
    var list = List[String]()
    list ::= ("Timings of " + this.getClass().toString())
    list ::= "description:" + times.map(x => "\t" + x._1).reduce(_ + _)
    list ::= "times(ms):" + times.map(x => "\t" + x._2).reduce(_ + _)
    list.reverse
  }

}

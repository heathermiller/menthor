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
    printToFile(new File(path))(p => {
      p.println("Timings of " + this.getClass())
      p.println(stringf("descr:", "time(ms):"))
      times.foreach(t => p.println(stringf(t._1, t._2.toString())))
    })
  }

  /**
   * Print the logged times.
   */
  def printTimesLog() {
    println("Timings of " + this.getClass())
    println(stringf("descr:", "time(ms):"))
    times.foreach(t => println(stringf(t._1, t._2.toString())))
  }

  /**
   * Helper method for writing to files.
   * @param f
   * @param op
   */
  private def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  /**
   * Defines a format string for the output of timings and their descriptions.
   * @param s
   * @param t
   * @return
   */
  private def stringf(s: String, t: String): String = {
    "%-20s%-20s".format(s, t)
  }
}

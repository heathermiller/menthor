package processing.parallel

import scala.collection.mutable.{Map, HashMap}
import scala.util.parsing.combinator._

import java.io.{FileWriter, BufferedWriter}
import java.util.concurrent.{ConcurrentMap, ConcurrentHashMap}

/* Represents a cluster that has been created by merging its `left` and `right` clusters.
 * `vec` is a list of word occurrence counts.
 */
class Cluster(val vec: List[Int], val left: Cluster = null, val right: Cluster = null, val id: Int)

object Clustering extends RegexParsers {

  override def skipWhitespace = false

  lazy val token: Parser[String] =
    """[\S| ]+""".r

  lazy val tab: Parser[String] =
    """\t""".r

  lazy val edgeline: Parser[List[String]] =
    repsep(token, tab)

  val vertices: Map[String, Vertex[Double]] = new HashMap[String, Vertex[Double]]

  def tokenize(line: String): List[String] =
    tokenize(line, x => throw new Exception(x))

  def tokenize(line: String, onError: String => Unit): List[String] =
    parse(edgeline, line.trim) match {
      case Success(args, _)     => args
      case NoSuccess(msg, rest) => onError(msg); List()
    }

  def pearson(v1: List[Int], v2: List[Int]): Double = {
    val sum = (x: Int, y: Int) => x + y

    val sum1 = v1.reduceLeft(sum)
    val sum2 = v2.reduceLeft(sum)
    val sum1Sq = (for (v <- v1) yield v * v).reduceLeft(sum)
    val sum2Sq = (for (v <- v2) yield v * v).reduceLeft(sum)
    val pSum = (for (i <- 0 until v1.size) yield v1(i) * v2(i)).reduceLeft(sum)

    // Pearson score
    val num = pSum - (sum1 * sum2 / v1.size)
    val den = math.sqrt((sum1Sq - (sum1 * sum1) / v1.size) * (sum2Sq - (sum2 * sum2) / v1.size))
    if (den == 0) 0.0
    else 1.0 - num / den
  }

  // sequential implementation
  def hcluster(rows: List[List[Int]]): Cluster = {
    val distances: Map[(Int, Int), Double] = new HashMap[(Int, Int), Double]
    var currentclustid = -1

    var clust: List[Cluster] = (for (i <- 0 until rows.size) yield new Cluster(vec = rows(i), id = i)).toList
    var iter = 0

    while (clust.size > 1) {
      iter += 1
      var lowestpair = (0, 1)
      var closest = Clustering.pearson(clust(0).vec, clust(1).vec)
 
      for (i <- 0 until clust.size) {
        for (j <- i + 1 until clust.size) {
          if (distances.get((clust(i).id, clust(j).id)).isEmpty) {
            distances.put((clust(i).id, clust(j).id), Clustering.pearson(clust(i).vec, clust(j).vec))
          }

          val d = distances((clust(i).id, clust(j).id))

          if (d < closest) {
            closest = d
            lowestpair = (i, j)
          }
        }
      }

      // calculate average
      val mergevec: List[Int] = (for (i <- 0 until clust(0).vec.size) yield
        (clust(lowestpair._1).vec(i) + clust(lowestpair._2).vec(i)) / 2).toList

      val newcluster = new Cluster(mergevec, clust(lowestpair._1), clust(lowestpair._2), currentclustid)

      currentclustid -= 1

      // List(id1, id2, id3)
      val id1ToRemove = clust(lowestpair._1).id
      val id2ToRemove = clust(lowestpair._2).id
      clust = clust.filterNot((c: Cluster) => c.id == id1ToRemove)
      // List(id2, id3)
      clust = clust.filterNot((c: Cluster) => c.id == id2ToRemove)

      clust = newcluster :: clust
    }

    clust(0)
  }

  def printCluster(clust: Cluster, labels: List[String], n: Int = 0) {
    for (i <- 0 until n) print(" ")
    if (clust.id < 0) {
      println("-")
    } else {
      if (labels == null) println(clust.id)
      else println(labels(clust.id))
    }
    if (clust.left != null) printCluster(clust.left, labels, n + 1)
    if (clust.right != null) printCluster(clust.right, labels, n + 1)
  }

  def createBlogList() {
    val lines = scala.io.Source.fromFile("src/data/syndic8feedlist.txt").getLines()
    var urls: List[String] = List()
    for (line <- lines; if line.startsWith("<link>")) {
      val url = line.substring(6, line.length - 7)
      urls = url :: urls
    }
    val fileWriter = new FileWriter("more-feeds.txt")
    val writer = new BufferedWriter(fileWriter)
    for (u <- urls) {
      writer.write(u)
      writer.newLine()
    }
    writer.close()
  }

  def runClustering() {
//    createBlogList()
//    return

    val lines = scala.io.Source.fromFile("src/data/blogdata.txt").getLines()

    var firstLine = true
    var colnames: List[String] = List()
    var rownames: List[String] = List()
    var data: List[List[Int]] = List()

    for (line <- lines) {
      val tokens = tokenize(line)
      if (firstLine) {
        for (token <- tokens.drop(1)) {
          colnames = token :: colnames
        }
        firstLine = false
        println("column names:")
        for (col <- colnames.take(10))
          println(col)
      } else {
        rownames = tokens(0) :: rownames
        data = tokens.drop(1).map((x: String) => x.toInt) :: data
      }
    }

    println("row names:")
    for (row <- rownames.take(10))
      println(row)

    val clust = hcluster(data)
    printCluster(clust, rownames)
  }

  def runClusteringParallel(datafile: String, inputsize: Int) {
    val lines = scala.io.Source.fromFile(datafile).getLines().take(inputsize)

    var firstLine = true
    var colnames: List[String] = List()
    var rownames: List[String] = List()
    var data: List[List[Int]] = List()

    for (line <- lines) {
      val tokens = tokenize(line)
      if (firstLine) {
        for (token <- tokens.drop(1)) {
          colnames = token :: colnames
        }
        firstLine = false
        println("Some column names:")
        for (col <- colnames.take(10))
          println(col)
      } else {
        rownames = tokens(0) :: rownames
        data = tokens.drop(1).map((x: String) => x.toInt) :: data
      }
    }

    println("Some row names:")
    for (row <- rownames.take(10))
      println(row)

    SharedData.allClusters = (for (i <- 0 until data.size) yield new Cluster(vec = data(i), id = i)).toList

    // create graph with floating vertices
    val g = new Graph[Cluster]
    for (i <- 0 until data.size) {
      val v = new ClusteringVertex(SharedData.allClusters, SharedData.allClusters(i), ""+i)
      g.addVertex(v)
    }
    g.start()

    g.iterateUntil({ SharedData.allClusters.size == 1 })
    g.terminate()

    // result is in SharedData
    printCluster(SharedData.allClusters(0), rownames)
  }

  var debug = false

  // scala -cp .. processing.parallel.Clustering par false data/blogdata.txt 100
  def main(args: Array[String]) {
    debug = args(1).toBoolean

    if (args(0) == "par")
      runClusteringParallel(args(2), args(3).toInt)
    else
      runClustering()
  }

}

object SharedData {
  val distanceTable: ConcurrentMap[(Int, Int), Double] = new ConcurrentHashMap[(Int, Int), Double]
  @volatile var kept: Int = 0 // cluster id
  @volatile var removed: Int = 0
  @volatile var allClusters: List[Cluster] = List()

  @volatile var startInit: Long = 0
  @volatile var endInit: Long = 0
}

/* Hierarchical clustering
 * 
 */
class ClusteringVertex(var clusters: List[Cluster], cluster: Cluster, label: String) extends Vertex[Cluster](label, cluster) {

  def numVertices = graph.vertices.size

  def merge() {
    // obtain cluster we merge with
    val toMergeWith: Cluster = SharedData.allClusters.find((c: Cluster) => c.id == SharedData.removed).get
        
    // calculate average
    val mergevec: List[Int] = (for (i <- 0 until value.vec.size) yield
      (value.vec(i) + toMergeWith.vec(i)) / 2).toList

    val newcluster = new Cluster(mergevec, value, toMergeWith, SharedData.allClusters(0).id - 1)

    val id1ToRemove = value.id
    val id2ToRemove = toMergeWith.id
    SharedData.allClusters = SharedData.allClusters.filterNot((c: Cluster) => c.id == id1ToRemove)
    SharedData.allClusters = SharedData.allClusters.filterNot((c: Cluster) => c.id == id2ToRemove)
    SharedData.allClusters = newcluster :: SharedData.allClusters
        
    // remove distance info from shared table for merged vertices
    SharedData.distanceTable.remove((id1ToRemove, id2ToRemove))

    if (Clustering.debug) {
      println("Created new merged cluster " + newcluster.id)
      println("size of allClusters: " + SharedData.allClusters.size)
    }

    // replace our own `Cluster` value
    value = newcluster
  }

  def update(superstep: Int, incoming: List[Message[Cluster]]): Substep[Cluster] = {
    {
      if (superstep == 0) {
        for (other <- clusters; if other != value) {
          val distance: Double = Clustering.pearson(value.vec, other.vec)
          SharedData.distanceTable.put((value.id, other.id), distance)
        }
      }
      List()
    } then { // TODO: implement `thenOne`
      if (this == graph.vertices(0)) {
        SharedData.endInit = System.currentTimeMillis()

        // find the minimal distance
        var closest = 1000000.0d
        var lowestpair = (0, 0)
        for (cluster1 <- SharedData.allClusters; cluster2 <- SharedData.allClusters; if cluster1 != cluster2;
             val pair = (cluster1.id, cluster2.id); if SharedData.distanceTable.containsKey(pair)) {
          val d = SharedData.distanceTable.get(pair)
          if (d < closest) {
            closest = d
            lowestpair = pair
          }
        }
        if (Clustering.debug)
          println("Found closest pair "+ lowestpair + " with dist " + closest)
        // store information about the merge in shared memory
        SharedData.kept = lowestpair._1
        SharedData.removed = lowestpair._2
      }
      List()
    } then {
      // TODO: remove vertex from graph, so it gets GC'ed
      if (SharedData.kept == value.id) { // do we have to merge?
        merge()
      } else if (SharedData.removed != value.id) {
        clusters = clusters.filterNot((c: Cluster) => c.id == SharedData.removed)
      }
      List()
    } then {
      // update distance to merged cluster
      val mergedCluster = SharedData.allClusters(0)
      if (value != mergedCluster) {
        val distance: Double = Clustering.pearson(value.vec, mergedCluster.vec)
        SharedData.distanceTable.put((value.id, mergedCluster.id), distance)
      }
      List()
    }
  }

}

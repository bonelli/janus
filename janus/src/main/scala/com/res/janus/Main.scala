/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.res.janus

import breeze.numerics._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.rdd.RDD

object Main {

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    if (false) {
      //    val filename = "C:\\Users\\bof\\Downloads\\bcsstk32.mtx"
      //    val filename = "C:\\Users\\bof\\Downloads\\bcsstk18.mtx\\bcsstk18.mtx"
      val filename = "C:\\Users\\bof\\Downloads\\bcsstk14.mtx\\bcsstk14.mtx"
      val m = new CSVSparseMatrix(filename, true)

      val lu = LU(m)

      println("max: " + abs(m.dense - lu.l.dense * lu.u.dense).max)
    }

    // ----------- ultra simple matrix
    //    val vals = List(
    //      ((1L, 1L), 1.0),
    //      ((7L, 1L), 2.0),
    //      ((8L, 1L), 3.0),
    //      ((9L, 1L), 4.0),
    //      ((2L, 2L), 5.0),
    //      ((4L, 2L), 6.0),
    //      ((6L, 2L), 7.0),
    //      ((3L, 3L), 8.0),
    //      ((5L, 3L), 9.0),
    //      ((8L, 3L), 1.0),
    //      ((4L, 4L), 2.0),
    //      ((8L, 4L), 3.0),
    //      ((9L, 4L), 4.0),
    //      ((5L, 5L), 5.0),
    //      ((6L, 5L), 6.0),
    //      ((8L, 5L), 7.0),
    //      ((6L, 6L), 8.0),
    //      ((9L, 6L), 9.0),
    //      ((7L, 7L), 1.0),
    //      ((8L, 7L), 2.0),
    //      ((9L, 7L), 3.0),
    //      ((8L, 8L), 4.0),
    //      ((9L, 9L), 5.0)
    //    )
    //
    //    val m = new MemorySparseMatrix(vals, true)

    // ------------------- moderately complicated matrix
    val filename = "C:\\Users\\bof\\Downloads\\bcsstk14.mtx\\bcsstk14.mtx"
    val m = new CSVSparseMatrix(filename, true)

    val method = new EliminationMethod(m)

    val logFile = "C:\\Users\\bof\\Downloads\\2 - 7 - Lecture 1.7 - Tail Recursion (12-32).txt" // Should be some file on your system
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    // actual work starts here

    val pivotsRDD: RDD[(VertexId, Long)] = sc.parallelize(method.pivots.map(p => (p.toLong, 0L))) ++ sc.parallelize(Array((-1L, 0L))) // add the super-root element
    val dependenciesRDD: RDD[Edge[Unit]] = sc.parallelize(method.childToParentEdges.toSeq.map { case (c, p) => Edge[Unit](c, p) }) ++ sc.parallelize(method.pivots.filterNot(x => method.childToParentEdges.contains(x)).map(r => Edge[Unit](r, -1L))) // add the super root
    val assemblyTree = Graph(pivotsRDD, dependenciesRDD)

    val solveTree = assemblyTree.outerJoinVertices(assemblyTree.inDegrees) { (id, oldAttr, inDegOpt) =>
      inDegOpt match {
        case Some(inDeg) => inDeg
        case None => 0 // No inDegree means zero inDegree
      }
    }

    //    solveTree.vertices.foreach(println(_))
    class NeighborsContributions(val amount: Long, val front: FrontContribution) extends Serializable

    // start solving
    val unsolvedFronts: Graph[(Long, FrontContribution), Unit] = solveTree.mapVertices((p, unsolvedChildren) => (unsolvedChildren, EmptyFrontContribution))
    val solved = unsolvedFronts
      .pregel[NeighborsContributions](
        initialMsg = new NeighborsContributions(0L, EmptyFrontContribution),
        //        maxIterations = 20,
        activeDirection = EdgeDirection.Out
      )(
        (id, data, incomingContributions) => { // receiving neighbor message
          println("Map vertex: " + id)
          if (incomingContributions.amount == 0) data // this is the first fake message, it will bring to 0 just those who were ready
          else { // this is the case where we actually received a child contribution
            val (unsolvedChildren, childrenContributions) = data
            //          println("Composing children contribution for pivot: " + id + " remaining children: " + (unsolvedChildren - incomingContributions.amount))
            (unsolvedChildren - incomingContributions.amount, childrenContributions + incomingContributions.front)
          }
        },
        triplet => { // function that sends messages
          println("Map triplet: " + triplet.srcId) //
          //        println("triplet: " + triplet)
          val pivot = triplet.srcId
          val (unsolvedChildren, childrenContributions) = triplet.srcAttr
          if (unsolvedChildren == 0) { // if we are ready
            //          println("Solving and sending message from pivot: " + pivot)
            val s = method.frontStart(pivot)
            require(
              childrenContributions.updateMatrix.indexes.size == 0 ||
                (s.indexes(0, 0)._1 <= childrenContributions.updateMatrix.indexes(0, 0)._1 && s.indexes(0, 0)._2 <= childrenContributions.updateMatrix.indexes(0, 0)._2),
              "front(pivot=" + pivot + ") = " + s.indexes + "\n" +
                "childrenUpdateMatrices = " + childrenContributions.updateMatrix.indexes
            )
            val currentFront = s + childrenContributions.updateMatrix
            Iterator((triplet.dstId, new NeighborsContributions(1L, currentFront.contribution.addChildContribution(childrenContributions))))
          } else { // if we were not ready
            //          println("Children not ready for pivot: " + pivot)
            Iterator.empty
          }
        },
        (child1Contribution, child2Contribution) => new NeighborsContributions(child1Contribution.amount + child2Contribution.amount, child1Contribution.front + child2Contribution.front)
      )
      .cache

    println(solved.vertices.filter { case (id, data) => data._1 == 0 }.count)
    if (true) {
      sc.stop
      return
    }

    val (fakeRootPivot, vertexData) = solved.vertices
      .filter { case (pivot: Long, vertexData: (Long, FrontContribution)) => pivot == -1 } // filter out all but the super-root
      .first // take that only super root

    val (unsolvedChildren, frontContribution) = vertexData

    val uMap = frontContribution.u
    val lMap = frontContribution.l
    val u = new MemorySparseMatrix(uMap, false)

    val lDiagonal = for {
      i <- 1L to Math.max(m.nRows, m.nCols)
    } yield (i, i) -> 1.0

    val l = new MemorySparseMatrix(lMap.mapValues(-_)
      .map(identity) // this is because of serialization bug https://issues.scala-lang.org/browse/SI-7005
      ++ lDiagonal, false)
    val lu = new LUFactorization(l, u)

    println(abs(m.dense - lu.l.dense * lu.u.dense).max)

    //    assemblyTree.vertices.foreach(println(_))
    //    assemblyTree.edges.foreach(println(_))

    // actual work ends here
    if (true) {
      sc.stop
      return
    }
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    // Count all users which are postdocs
    println("postdocs:" + graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count)
    // Count all the edges where src > dst
    println("bah: " + graph.edges.filter(e => e.srcId > e.dstId).count)
    //    val logData = sc.textFile(logFile, 2).cache()
    //    val numAs = logData.filter(line => line.contains("a")).count()
    //    val numBs = logData.filter(line => line.contains("b")).count()
    //    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    //
    graph.vertices.foreach(println(_))
    graph.edges.foreach(println(_))
    sc.stop
  }

}

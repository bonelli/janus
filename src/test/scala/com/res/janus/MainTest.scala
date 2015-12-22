/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.res.janus

//import org.scalatest.junit.AssertionsForJUnit
import org.junit._
import Assert._
import breeze.numerics._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.rdd.RDD

class MainTest {

  @Before
  def setUp: Unit = {
  }

  @After
  def tearDown: Unit = {
  }

  @Test
  def pregelSolve: Unit = {
    println("pregelSolve")

    val filename = "test-data/bcsstk14.mtx"
    val m = new CSVSparseMatrix(filename, true)

    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[1]")

    val sc = new SparkContext(conf)

    val method = new EliminationMethod(m)
    // actual work starts here

    val pivotsRDD: RDD[(VertexId, Long)] = sc.parallelize(method.pivots.map(p => (p.toLong, 0L))) ++ sc.parallelize(Array((-1L, 0L))) // add the super-root element
    val dependenciesRDD: RDD[Edge[Unit]] = sc.parallelize(method.childToParentEdges.toSeq.map { case (c, p) => Edge[Unit](c, p) }) ++ sc.parallelize(method.pivots.filterNot(x => method.childToParentEdges.contains(x)).map(r => Edge[Unit](r, -1L))) // add the super root
    val assemblyTree = Graph(pivotsRDD, dependenciesRDD)

    val solveTree = assemblyTree.outerJoinVertices(assemblyTree.inDegrees) { (id, oldAttr, inDegOpt) =>
      inDegOpt match {
        case Some(inDeg) => inDeg
        case None        => 0 // No inDegree means zero inDegree
      }
    }

    //    solveTree.vertices.foreach(println(_))
    
    // start solving
    val unsolvedFronts: Graph[(Long, FrontContribution), Unit] = solveTree.mapVertices((p, unsolvedChildren) => (unsolvedChildren, EmptyFrontContribution))
    val solved = unsolvedFronts
      .pregel[NeighborsContributions](
        initialMsg = new NeighborsContributions(0L, EmptyFrontContribution),
        //        maxIterations = 20,
        activeDirection = EdgeDirection.Out)(
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
                  "childrenUpdateMatrices = " + childrenContributions.updateMatrix.indexes)
              val currentFront = s + childrenContributions.updateMatrix
              Iterator((triplet.dstId, new NeighborsContributions(1L, currentFront.contribution.addChildContribution(childrenContributions))))
            } else { // if we were not ready
              //          println("Children not ready for pivot: " + pivot)
              Iterator.empty
            }
          },
          (child1Contribution, child2Contribution) => new NeighborsContributions(child1Contribution.amount + child2Contribution.amount, child1Contribution.front + child2Contribution.front))
      .cache

    println(solved.vertices.filter { case (id, data) => data._1 == 0 }.count)

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

    val errNorm = abs(m.dense - lu.l.dense * lu.u.dense).max
    println("l-inf error norm: " + errNorm)

    //    assemblyTree.vertices.foreach(println(_))
    //    assemblyTree.edges.foreach(println(_))

    // actual work ends here
    assertEquals(0.0, errNorm, 1e-14)

    sc.stop
  }

}

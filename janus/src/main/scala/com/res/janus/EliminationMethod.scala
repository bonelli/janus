/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.res.janus

import breeze.linalg.DenseMatrix
import com.res.janus.MatrixUtils._

class FrontContribution(val updateMatrix: FrontMatrix, val l: Map[(Long, Long), Double], val u: Map[(Long, Long), Double]) extends Serializable {
  def +(that: FrontContribution): FrontContribution = {
    require(that.l.keySet intersect this.l.keySet isEmpty, "L1 = " + l + "\nL2 = " + that.l)
    require(that.u.keySet intersect this.u.keySet isEmpty, "U1 = " + l + "\nU2 = " + that.l)
    new FrontContribution(updateMatrix + that.updateMatrix, l ++ that.l, u ++ that.u)
  }

  def addChildContribution(that: FrontContribution): FrontContribution = {
    require(that.l.keySet intersect this.l.keySet isEmpty, "L1 = " + l + "\nL2 = " + that.l)
    require(that.u.keySet intersect this.u.keySet isEmpty, "U1 = " + l + "\nU2 = " + that.l)
    new FrontContribution(updateMatrix, l ++ that.l, u ++ that.u)
  }

  override def toString = "L = " + l + "\nU = " + u
}

class FrontMatrix(val dense: DenseMatrix[Double], val indexes: DenseMatrix[(Long, Long)]) extends Serializable {
  require(dense.size == 0 || dense(0, 0) != 0.0, "Null pivot on front: " + dense + " for indexes: " + indexes)

  lazy val pivotValue = dense(0, 0)
  lazy val updateMatrix: FrontMatrix = new FrontMatrix(updateDenseMatrix, updateMatrixIndexes(indexes))
  lazy val factorizationMatrix: DenseMatrix[Double] = DenseMatrix.tabulate(dense.rows, dense.cols) {
    case (i, j) => {
      if (i == j) 1.0
      else if (j == 0) dense(i, 0) / -pivotValue
      else 0.0
    }
  }

  lazy val factorized: DenseMatrix[Double] = factorizationMatrix * dense
  lazy val updateDenseMatrix: DenseMatrix[Double] = factorized(1 until dense.rows, 1 until dense.cols)

  lazy val upperContribution: Map[(Long, Long), Double] = indexes(0, ::).inner.toArray.zipWithIndex.map { case (originalIndex, j) => originalIndex -> factorized(0, j) }.toMap
  lazy val lowerContribution: Map[(Long, Long), Double] = indexes(1 until dense.rows, 0).toArray.zipWithIndex.map { case (originalIndex, i) => originalIndex -> factorizationMatrix(i + 1, 0) }.toMap

  def contribution = new FrontContribution(updateMatrix, lowerContribution, upperContribution)

  private def updateMatrixIndexes(m: DenseMatrix[(Long, Long)]): DenseMatrix[(Long, Long)] = {
    DenseMatrix.tabulate(m.rows - 1, m.cols - 1) {
      case (i, j) => (row(m(i + 1, 0)), col(m(0, j + 1)))
    }
  }

  lazy val mappingMatrix: DenseMatrix[((Long, Long), Double)] = dense.mapPairs { case ((i, j), v) => (indexes(i, j), v) }
  lazy val mappingMap: Map[(Long, Long), Double] = mappingMatrix.toArray.toMap.filterKeys(_ != null)
    .map(identity) // this is because of serialization bug https://issues.scala-lang.org/browse/SI-6654

  private def extendedAddMap(that: FrontMatrix): Map[(Long, Long), Double] = {
    val thatMappingMap = that.mappingMap
    def myMappingMap = mappingMap
    myMappingMap.keySet.union(thatMappingMap.keySet).toIterable.map { case k => (k, myMappingMap.getOrElse(k, 0.0) + thatMappingMap.getOrElse(k, 0.0)) }.toMap
  }

  def +(that: FrontMatrix): FrontMatrix = {
    val addMap = extendedAddMap(that).withDefaultValue(0.0)
    val rows = addMap.keys.map(_._1).toList.sorted
    val cols = addMap.keys.map(_._2).toList.sorted
    val sumFront = DenseMatrix.tabulate(rows.size, cols.size) {
      case (i, j) => addMap(rows(i), cols(j))
    }
    val sumIndexes = DenseMatrix.tabulate(rows.size, cols.size) {
      case (i, j) => (rows(i), cols(j))
    }
    new FrontMatrix(sumFront, sumIndexes)
  }

  override def toString = mappingMatrix.toString
}

class LUFactorization(val l: SparseMatrix, val u: SparseMatrix)

class EliminationMethod(matrix: SparseMatrix) extends EliminationGraph(matrix) {

  def frontStart(pivot: Long): FrontMatrix = {
    val indexes = frontStartIndexes(pivot)
    new FrontMatrix(indexes.mapValues(matrix.apply)
      .map(identity) // this is because of serialization bug https://issues.scala-lang.org/browse/SI-7005
      , indexes)
  }

  def front(node: EliminationTreeNode): FrontMatrix = {
    val s = frontStart(node.pivot)
    val childrenUpdates = node.children.map(c => front(c).updateMatrix).foldLeft[FrontMatrix](EmptyFrontMatrix)(_ + _)
    s + childrenUpdates
  }

  def solve(node: EliminationTreeNode): FrontContribution = {
    println("Composing front: " + node.pivot)
    val childrenContributions = node.children.map(c => solve(c)).foldLeft[FrontContribution](EmptyFrontContribution)(_ + _)
    val s = frontStart(node.pivot)
    require(
      childrenContributions.updateMatrix.indexes.size == 0 ||
        (s.indexes(0, 0)._1 <= childrenContributions.updateMatrix.indexes(0, 0)._1 && s.indexes(0, 0)._2 <= childrenContributions.updateMatrix.indexes(0, 0)._2),
      "front(pivot=" + node.pivot + ") = " + s.indexes + "\n" +
        "childrenUpdateMatrices = " + childrenContributions.updateMatrix.indexes + "\n" +
        "node was = " + node
    )
    val currentFront = s + childrenContributions.updateMatrix
    currentFront.contribution.addChildContribution(childrenContributions)
  }

  def solve: LUFactorization = {
    val (lMap, uMap) = roots // for each independent tree
      .map(solve) // solve it
      // fold together all of the l and u values coming from the different elimination trees
      .foldRight((Map[(Long, Long), Double](), Map[(Long, Long), Double]())) { case (contribution, (l, u)) => (contribution.l ++ l, contribution.u ++ u) }
    val u = new MemorySparseMatrix(uMap, false)

    val lDiagonal = for {
      i <- 1L to Math.max(matrix.nRows, matrix.nCols)
    } yield (i, i) -> 1.0

    val l = new MemorySparseMatrix(lMap.mapValues(-_)
      .map(identity) // this is because of serialization bug https://issues.scala-lang.org/browse/SI-7005
      ++ lDiagonal, false)
    new LUFactorization(l, u)
  }

}

object EmptyFrontMatrix extends FrontMatrix(DenseMatrix.zeros[Double](0, 0), DenseMatrix.zeros[(Long, Long)](0, 0))
object EmptyFrontContribution extends FrontContribution(EmptyFrontMatrix, Map(), Map())

object LU {
  def apply(m: SparseMatrix): LUFactorization = new EliminationMethod(m).solve
}
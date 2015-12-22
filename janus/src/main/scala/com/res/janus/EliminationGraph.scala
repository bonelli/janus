/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.res.janus

import breeze.linalg.DenseMatrix
import com.res.janus.MatrixUtils._
import scala.collection.immutable.SortedSet

class EliminationGraph(nnzIndexes: SparsityPattern) extends Serializable {
  private lazy val nnz = SortedSet[(Long, Long)]() ++ nnzIndexes.nnz
  def pivots = (1L to nnzIndexes.nCols)

  //  // map row -> cols
  //  private lazy val filledCols = {
  //    if (nnzIndexes.symmetric) filledRows
  //    else {
  //      eliminationNnz
  //        .filter(aboveDiagonal)
  //        .groupBy(row)
  //        .map({ case (k, v) => (k, v.map(col)) })
  //        .withDefaultValue(Stream())
  //    }
  //  }
  //
  //  // map col -> rows
  //  private lazy val filledRows = {
  //    eliminationNnz
  //      .filter(belowDiagonal)
  //      .groupBy(col)
  //      .map({ case (k, v) => (k, v.map(row)) })
  //      .withDefaultValue(Stream())
  //  }
  private lazy val effectiveNNZ = {
    if (nnzIndexes.symmetric) nnz ++ nnz.map { case (i, j) => (j, i) }
    else nnz
  }

  private lazy val (filledRows, filledCols) = {

    val startRowsFilled = effectiveNNZ
      .filter(belowDiagonal)
      .groupBy(col)
      .mapValues(_.map(row))
      .map(identity) // this is because of serialization bug https://issues.scala-lang.org/browse/SI-7005
      .withDefaultValue(Set[Long]())

    val startColsFilled = effectiveNNZ
      .filter(aboveDiagonal)
      .groupBy(row)
      .mapValues(_.map(col))
      .map(identity) // this is because of serialization bug https://issues.scala-lang.org/browse/SI-7005
      .withDefaultValue(Set[Long]())

    //    println(startRowsFilled)
    //    println(startColsFilled)

    def eliminationNnz2(remainingPivots: Seq[Long], rowsFilled: Map[Long, Set[Long]], colsFilled: Map[Long, Set[Long]]): (Map[Long, Set[Long]], Map[Long, Set[Long]]) = {
      if (remainingPivots.isEmpty) (rowsFilled, colsFilled)
      else {
        val pivot = remainingPivots.head
        val pivotRows = rowsFilled(pivot).toList
        val pivotCols = colsFilled(pivot).toList
        //        val pivotFillins = pivotRows.flatMap(i => pivotCols.map(j => (i, j)))

        println("composing nnz for pivot: " + pivot)

        def addRowsFilled(i: Iterable[Long], j: Long, rowsFilled: Map[Long, Set[Long]]): Map[Long, Set[Long]] = {
          rowsFilled + (j -> (rowsFilled(j) ++ i))
        }
        def addColsFilled(i: Long, j: Iterable[Long], colsFilled: Map[Long, Set[Long]]): Map[Long, Set[Long]] = {
          colsFilled + (i -> (colsFilled(i) ++ j))
        }
        val newColsFilled = pivotRows
          .map(i => (i, pivotCols.filter(_ > i)))
          .foldRight(colsFilled) { case ((i, js), cf) => addColsFilled(i, js, cf) }
        //        val newColsFilled = pivotFillins
        //          .filter(aboveDiagonal)
        //          .groupBy(row)
        //          .mapValues(_.map(col))
        //          .foldRight(colsFilled) { case ((i, js), cf) => addColsFilled(i, js, cf) }

        val newRowsFilled = pivotCols
          .map(j => (j, pivotRows.filter(_ > j)))
          .foldRight(rowsFilled) { case ((j, is), rf) => addRowsFilled(is, j, rf) }
        //        val newRowsFilled = pivotFillins
        //          .filter(belowDiagonal)
        //          .groupBy(col)
        //          .mapValues(_.map(row))
        //          .foldRight(rowsFilled) { case ((j, is), rf) => addRowsFilled(is, j, rf) }

        eliminationNnz2(remainingPivots.tail, newRowsFilled, newColsFilled)
      }
      //          if (remainingPivots.isEmpty) partialNnzL
      //      Set[(Long, Long)]()
    }

    eliminationNnz2(pivots, startRowsFilled, startColsFilled)
  }

  //  private def pivotFilledRows(stepNnz: Iterable[(Long, Long)], p: Long) = {
  //    stepNnz // select nnz
  //      .filter(col(_) == p) // belonging to this pivot
  //      .filter(belowDiagonal) // below the diagonal
  //      .map { case (i, j) => i }
  //
  //  }
  //  private def pivotFilledCols(stepNnz: Iterable[(Long, Long)], p: Long) = {
  //    if (nnzIndexes.symmetric) pivotFilledRows(stepNnz, p)
  //    else stepNnz
  //      .filter(row(_) == p)
  //      .filter(aboveDiagonal)
  //      .map { case (i, j) => j }
  //  }

  //  private def nextStepNNZ(stepNnz: Set[(Long, Long)], p: Long): Set[(Long, Long)] = {
  //    println(stepNnz.getClass)
  //    //    val stepNnzList = stepNnz.toList
  //    val filledRows = pivotFilledRows(stepNnz, p)
  //    val filledCols = {
  //      if (nnzIndexes.symmetric) filledRows
  //      else pivotFilledCols(stepNnz, p)
  //    }
  //
  //    val fillins = filledRows.flatMap(i => filledCols.map(j => (i, j)))
  //    stepNnz ++ fillins
  //  }

  lazy val eliminationNnz = effectiveNNZ ++ pivots.flatMap(p => filledRows(p).flatMap(i => filledCols(p).map(j => (i, j))))

  lazy val nnzL = (filledRows.toIterable.flatMap { case (j, is) => is.map(i => (i, j)) } ++ pivots.map(p => (p, p))).toSet

  // this map goes from child to parent
  lazy val childToParentEdges = {
    filledRows.filterNot { case (j, is) => is.isEmpty }.mapValues(_.min)
      .map(identity) // this is because of serialization bug https://issues.scala-lang.org/browse/SI-7005
  }

  lazy val parentToChildEdges = {
    childToParentEdges
      .toIterable
      .map(_.swap)
      .groupBy(_._1)
      .map({ case (k, v) => (k, v.map(_._2)) })
      .withDefaultValue(Nil) // if somebody asks for a parent with no children, the children list is a Nil
  }

  // this sequence has all columns which are not child of any other column in the eliminationGraph
  lazy val rootNodes = pivots.filterNot(x => childToParentEdges.contains(x))

  def tree(pivot: Long): EliminationTreeNode = {

    def childrenSet(pivot: Long): Stream[EliminationTreeNode] = {
      parentToChildEdges(pivot).toStream.map(tree)
    }

    new EliminationTreeNode(pivot, childrenSet(pivot))
  }

  def roots(): Seq[EliminationTreeNode] = rootNodes.map(tree)

  def frontStartIndexes(pivot: Long): DenseMatrix[(Long, Long)] = {
    val cols = filledCols(pivot).toVector
    val rows = filledRows(pivot).toVector

    DenseMatrix.tabulate(rows.size + 1, cols.size + 1) {
      case (0, 0) => (pivot, pivot)
      case (0, j) => (pivot, cols(j - 1))
      case (i, 0) => (rows(i - 1), pivot)
      case (i, j) => null
    }
  }

}

class EliminationTreeNode(val pivot: Long, val children: Stream[EliminationTreeNode]) {
  def size: Long = 1L + children.foldRight(0L)((child, part) => part + child.size)
  def maxBranching: Long = (children.size.toLong :: children.map(_.maxBranching).toList).max
  lazy val descendants: Map[Long, EliminationTreeNode] = {
    val directChildren = children.map(c => c.pivot -> c).toMap
    children.foldRight(directChildren)((directChild, acc) => directChild.descendants ++ acc)
  }
  lazy val descendantPivots: Set[Long] = descendants.keySet
  //  def descendant() : Set[EliminationTreeNode] = {
  //  	children union (children.map( c => c.descendant() ).toSet)
  //  }
  override def toString = "Node(" + pivot + "," + children + ")"
}
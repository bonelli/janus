/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.res.janus
import com.res.janus.MatrixUtils._

class MemorySparsityPattern(val nnz: Iterable[(Long, Long)], val symmetric: Boolean) extends SparsityPattern with Serializable {
  lazy val nRows = nnz.map(row).toList.distinct.size.toLong
  lazy val nCols = nnz.map(col).toList.distinct.size.toLong
  lazy val numNnz = nnz.size.toLong
}

class MemorySparseMatrix(vals: Iterable[((Long, Long), Double)], symmetric: Boolean)
  extends MemorySparsityPattern(vals.map(_._1), symmetric)
  with SparseMatrix
  with Serializable {
  lazy val values = vals.toMap.withDefaultValue(0.0)
}
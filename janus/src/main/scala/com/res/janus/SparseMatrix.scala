/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.res.janus

import breeze.linalg.DenseMatrix

trait SparseMatrix extends SparsityPattern {
  def values: Map[(Long, Long), Double]
  def apply(index: (Long, Long)): Double = {
    if (index == null) 0.0
    else {
      val (i, j) = index
      if (symmetric && i < j) { values(index.swap) }
      else { values(index) }
    }
  }
  def dense = DenseMatrix.tabulate(nRows.toInt, nCols.toInt) {
    case (i, j) => this(i + 1L, j + 1L)
  }

}

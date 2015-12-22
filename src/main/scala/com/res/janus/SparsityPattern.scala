/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.res.janus

trait SparsityPattern extends Serializable {
  // if symmetric is true the nnz will be only present below the diagonal (lower triangular representation)
  def symmetric: Boolean
  def nnz: Iterable[(Long, Long)]
  def nRows: Long
  def nCols: Long
  def numNnz: Long

}

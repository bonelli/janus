/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.res.janus

object MatrixUtils {
  def aboveDiagonal(t: (Long, Long)): Boolean = row(t) < col(t)
  def belowDiagonal(t: (Long, Long)): Boolean = row(t) > col(t)
  def row(t: (Long, Long)): Long = t._1
  def col(t: (Long, Long)): Long = t._2
}


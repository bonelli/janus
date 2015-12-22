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

class EliminationMethodTest {

  @Before
  def setUp: Unit = {
  }

  @After
  def tearDown: Unit = {
  }

  @Test
  def luSingleEliminationTree = {
    println("luSingleEliminationTree")
    val vals = List(
      ((1L, 1L), 1.0),
      ((7L, 1L), 2.0),
      ((8L, 1L), 3.0),
      ((9L, 1L), 4.0),
      ((2L, 2L), 5.0),
      ((4L, 2L), 6.0),
      ((6L, 2L), 7.0),
      ((3L, 3L), 8.0),
      ((5L, 3L), 9.0),
      ((8L, 3L), 1.0),
      ((4L, 4L), 2.0),
      ((8L, 4L), 3.0),
      ((9L, 4L), 4.0),
      ((5L, 5L), 5.0),
      ((6L, 5L), 6.0),
      ((8L, 5L), 7.0),
      ((6L, 6L), 8.0),
      ((9L, 6L), 9.0),
      ((7L, 7L), 1.0),
      ((8L, 7L), 2.0),
      ((9L, 7L), 3.0),
      ((8L, 8L), 4.0),
      ((9L, 9L), 5.0)
    )

    val m = new MemorySparseMatrix(vals, true)
    val lu = LU(m)
    assertEquals(0.0, abs(m.dense - lu.l.dense * lu.u.dense).max, 1e-14)
  }

  @Test
  def luDoubleEliminationTree = {
    println("luDoubleEliminationTree")
    val vals = List(
      ((1L, 1L), 1.0),
      ((7L, 1L), 2.0),
      ((8L, 1L), 3.0),
      ((9L, 1L), 4.0),
      ((2L, 2L), 5.0),
      ((4L, 2L), 6.0),
      ((6L, 2L), 7.0),
      ((3L, 3L), 8.0),
      ((5L, 3L), 9.0),
      ((8L, 3L), 1.0),
      ((4L, 4L), 2.0),
      ((8L, 4L), 3.0),
      ((9L, 4L), 4.0),
      ((5L, 5L), 5.0),
      ((6L, 5L), 6.0),
      ((8L, 5L), 7.0),
      ((6L, 6L), 8.0),
      ((9L, 6L), 9.0),
      ((7L, 7L), 1.0),
      ((8L, 7L), 2.0),
      ((9L, 7L), 3.0),
      ((8L, 8L), 4.0),
      ((9L, 9L), 5.0)
    )

    // add to the original values the same shifted, so that 2 independent blocks will form 2 different elimination trees
    val doubleVals = vals ++ vals.map { case ((i, j), v) => ((9 + i, 9 + j), v) }

    val m = new MemorySparseMatrix(doubleVals, true)
    val lu = LU(m)
    assertEquals(0.0, abs(m.dense - lu.l.dense * lu.u.dense).max, 1e-14)
  }

  @Test
  def lNnz = {
    println("lNnz")
    val vals = List(
      ((1L, 1L), 1.0),
      ((2L, 1L), 2.0),
      ((4L, 1L), 3.0),
      ((2L, 2L), 4.0),
      ((3L, 2L), 5.0),
      ((3L, 3L), 6.0),
      ((4L, 4L), 7.0)
    )

    val m = new MemorySparseMatrix(vals, true)
    val nnz = new EliminationMethod(m).nnzL

    //    println(nnz)
    assertEquals(
      Set(
        (1L, 1L),
        (2L, 1L),
        (4L, 1L),
        (2L, 2L),
        (3L, 2L),
        (4L, 2L),
        (3L, 3L),
        (4L, 3L),
        (4L, 4L)
      ),
      nnz
    )
  }

  @Test
  def luEliminationTreeLNNZComplicated = {
    println("luEliminationTreeLNNZComplicated")
    val vals = List(
      ((1L, 1L), 1.0),
      ((2L, 1L), 2.0),
      ((4L, 1L), 3.0),
      ((2L, 2L), 5.0),
      ((3L, 2L), 5.0),
      ((3L, 3L), 6.0),
      ((4L, 4L), 7.0)
    )

    val m = new MemorySparseMatrix(vals, true)
    println(new EliminationMethod(m).eliminationNnz.toList.sorted)

    val lu = LU(m)
    assertEquals(0.0, abs(m.dense - lu.l.dense * lu.u.dense).max, 1e-14)
  }

}

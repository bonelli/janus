/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.res.janus

import scala.io.Source

class CSVSparsityPattern(filename: String, val symmetric: Boolean) extends SparsityPattern {

  def csvSeparatorRegex = "\\s+"

  private def splittedLine(l: String) = l.split(csvSeparatorRegex)
  private def splittedHeaderLine(l: String) = splittedLine(l).take(3).map(_.toLong)

  private def lines = Source.fromFile(filename).getLines().filterNot(_ startsWith "%%")
  private val headerLine = splittedHeaderLine(lines.next())
  private def nnzLines = lines.drop(1)

  private def parseNNZLine(v: Array[String]): (Long, Long) = (v(0).toLong, v(1).toLong)
  // private def splittedNNZLine(l: String): (Long, Long) = parseNNZLine(splittedLine(l))
  protected def splittedNNZLines = nnzLines.map(splittedLine)

  val (nRows, nCols, numNnz) = (headerLine(0), headerLine(1), headerLine(2))
//  lazy val nnz = splittedNNZLines.map(parseNNZLine).toStream
  lazy val nnz = splittedNNZLines.map(parseNNZLine).toList

  override def toString = "MatrixNNZIndexes(" + nRows + "x" + nCols + ", nnz=" + numNnz + ")"

}

class CSVSparseMatrix(filename: String, symmetric: Boolean) extends CSVSparsityPattern(filename, symmetric) with SparseMatrix {
  def valueLineParser(l: Array[String]): ((Long, Long), Double) = ((l(0).toLong, l(1).toLong), l(2).toDouble)

  lazy val values = splittedNNZLines.map(valueLineParser).toMap.withDefaultValue(0.0)

}


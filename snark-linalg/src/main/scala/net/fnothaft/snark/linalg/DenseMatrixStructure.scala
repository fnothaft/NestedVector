/**
 * Copyright 2014 Frank Austin Nothaft
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.fnothaft.snark.linalg

import net.fnothaft.snark.DenseArrayStructure

object DenseMatrixStructure {

  def apply(h: Int, w: Int): DenseMatrixStructure = {
    DenseMatrixStructure(new DenseArrayStructure((0 to h).map(v => w.toLong).toSeq))
  }
}

case class DenseMatrixStructure(private[linalg] val structure: DenseArrayStructure) extends MatrixStructure {

  assert(structure.nestLengths.forall(_ == structure.nestLengths.head))

  // matrix height and width
  val h: Int = structure.nests
  val w: Int = structure.nestLengths.head.toInt

  def transpose(): MatrixStructure = {
    DenseMatrixStructure(new DenseArrayStructure((0 to w).map(i => h.toLong).toSeq))
  }

  def equals(mat: MatrixStructure): Boolean = mat match {
    case dm: DenseMatrixStructure => h == dm.h && w == dm.w
    case _                        => false
  }

  def toSparse(): SparseMatrixStructure = SparseMatrixStructure(h, w)
}

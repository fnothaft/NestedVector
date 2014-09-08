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

import net.fnothaft.snark.SparseArrayStructure

case class SparseMatrixStructure(override val h: Int,
                                 override val w: Int) extends MatrixStructure {
  def transpose(): MatrixStructure = {
    SparseMatrixStructure(w, h)
  }

  def equals(mat: MatrixStructure): Boolean = mat match {
    case sm: SparseMatrixStructure => h == sm.h && w == sm.w
    case _                         => false
  }

  def toStructure(): SparseArrayStructure = {
    new SparseArrayStructure((0 to h).map(i => (i, w.toLong)).toMap)
  }
}

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

import org.apache.spark.mllib.linalg.{ Vector => SparkVector }

abstract class Matrix(private[linalg] val structure: MatrixStructure) extends Serializable {

  protected def canMultiply(mat: Matrix): Boolean = structure.h == mat.structure.w

  def matrixAdd(mat: Matrix): Matrix

  def scalarMultiply(scalar: Double): Matrix

  def matrixMultiply(mat: Matrix): Matrix

  def vectorMultiply(vec: SparkVector): SparkVector
}

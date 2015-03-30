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
package net.fnothaft.snark

import scala.math.max

private[snark] class SparseArrayStructure(val nestLengths: Map[Int, Long]) extends ArrayStructure {

  private val indices = nestLengths.keys
    .zipWithIndex
    .toMap

  assert(nestLengths.size > 0, "Array must have at least one nest.")

  def nests: Int = nestLengths.keys.reduce(_ max _) + 1

  def elements: Long = nestLengths.values.reduce(_ + _)

  def getIndex(idx: NestedIndex): Option[Int] = indices.get(idx.nest)

  override def toString: String = nestLengths.toString

  override def hashCode(): Int = nestLengths.hashCode

  override def equals(other: Any): Boolean = other match {
    case das: DenseArrayStructure  => das.equals(this)
    case sas: SparseArrayStructure => true
    case _                         => false
  }

}

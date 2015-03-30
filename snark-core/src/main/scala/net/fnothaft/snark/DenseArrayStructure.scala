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

private[snark] class DenseArrayStructure(val nestLengths: Seq[Long]) extends ArrayStructure {

  assert(nestLengths.length > 0, "Array must have at least one nest.")

  def nests: Int = nestLengths.length

  def elements: Long = nestLengths.reduce(_ + _)

  def getIndex(idx: NestedIndex): Option[Int] = {
    if (idx.nest < nests && idx.idx < nestLengths(idx.nest)) {
      Some(idx.nest)
    } else {
      None
    }
  }

  override def toString: String = nestLengths.map(_.toString).reduce(_ + ", " + _)

  override def equals(other: Any): Boolean = other match {
    case das: DenseArrayStructure => {
      nestLengths.length == das.nestLengths.length &&
        nestLengths.zip(das.nestLengths).forall(p => p._1 == p._2)
    }
    case sas: SparseArrayStructure => {
      sas.nestLengths.forall(kv => {
        val (idx, len) = kv
        idx < nests && len <= nestLengths(idx)
      })
    }
    case _ => false
  }

  override def hashCode(): Int = nestLengths.hashCode

}

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

case class NestedIndex(nest: Int, idx: Int) extends Ordered[NestedIndex] {
  def compare(that: NestedIndex): Int = {
    val nestCompare = nest.compare(that.nest)

    if (nestCompare != 0) {
      nestCompare
    } else {
      idx.compare(that.idx)
    }
  }

  def lteq(that: NestedIndex): Boolean = !gt(that)

  def lt(that: NestedIndex): Boolean = nest < that.nest || (nest == that.nest && idx < that.idx)

  def eq(that: NestedIndex): Boolean = nest == that.nest && idx == that.idx

  def gt(that: NestedIndex): Boolean = nest > that.nest || (nest == that.nest && idx > that.idx)

  override def toString(): String = "NestedIndex(" + nest + ", " + idx + ")"
}

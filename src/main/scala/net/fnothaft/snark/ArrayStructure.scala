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

private[snark] case class ArrayStructure(nestLengths: Seq[Long]) {

  assert(nestLengths.length > 0, "Array must have at least one nest.")

  def equals(them: ArrayStructure): Boolean = {
    nestLengths.length == them.nestLengths.length &&
      nestLengths.zip(them.nestLengths).forall(p => p._1 == p._2)
  }

  def nests: Int = nestLengths.length

  def elements: Long = nestLengths.reduce(_ + _)

}

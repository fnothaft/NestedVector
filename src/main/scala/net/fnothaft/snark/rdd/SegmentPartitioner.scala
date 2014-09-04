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
package net.fnothaft.snark.rdd

import net.fnothaft.snark.{ ArrayStructure, NestedIndex }
import org.apache.spark.Partitioner

private[rdd] class SegmentPartitioner(structure: ArrayStructure) extends Partitioner {

  val nests = structure.nests

  def getPartition(key: Any): Int = key match {
    case ni: NestedIndex => if (ni.nest < nests && ni.idx <= structure.nestLengths(ni.nest)) {
      ni.nest
    } else {
      throw new IllegalArgumentException("Recieved out of range key: " + ni +
        ". Only have " + nests + " nests.")
    }
    case _ => throw new IllegalArgumentException("Received key with non nested-index type: " + key)
  }

  def numPartitions: Int = nests
}

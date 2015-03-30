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

import net.fnothaft.snark.{ DenseArrayStructure, NestedIndex }
import org.apache.spark.Partitioner
import scala.annotation.tailrec

private[rdd] class UniformPartitioner(structure: DenseArrayStructure,
                                      partitions: Int) extends Partitioner {

  val nests = structure.nests
  val partitionSize: Long = (structure.elements / partitions.toLong)

  @tailrec private def advanceBy(currentIndex: NestedIndex,
                                 advance: Long): NestedIndex = {
    assert(currentIndex.nest < nests)
    val distanceToEndOfNest = structure.nestLengths(currentIndex.nest) - currentIndex.idx
    if (advance < 0) {
      throw new IllegalArgumentException("Cannot advance a negative distance")
    } else if (advance <= distanceToEndOfNest) {
      NestedIndex(currentIndex.nest, currentIndex.idx)
    } else {
      advanceBy(NestedIndex(currentIndex.nest + 1, 0), advance - distanceToEndOfNest)
    }
  }

  @tailrec private def generateElements(partition: Int,
                                        maxPartitions: Int,
                                        partitionHeads: List[(Int, NestedIndex)],
                                        currentIndex: NestedIndex): List[(Int, NestedIndex)] = {
    if (partition >= maxPartitions) {
      partitionHeads
    } else {
      val newHeads = (partition, currentIndex) :: partitionHeads
      val newIndex = advanceBy(currentIndex, partitionSize)

      generateElements(partition + 1, maxPartitions, newHeads, newIndex)
    }
  }

  val partitionIndices = generateElements(0, partitions, List(), NestedIndex(0, 0))
    .toSeq
    .sortBy(kv => kv._1)
    .map(kv => kv._2)

  def getPartition(key: Any): Int = key match {
    case ni: NestedIndex => if (ni.nest < nests && ni.idx <= structure.nestLengths(ni.nest)) {
      partitionIndices.takeWhile(_.lteq(ni)).size - 1
    } else {
      throw new IllegalArgumentException("Recieved out of range key: " + ni +
        ". Only have " + nests + " nests.")
    }
    case _ => throw new IllegalArgumentException("Received key with non nested-index type: " + key)
  }

  def numPartitions: Int = partitions
}

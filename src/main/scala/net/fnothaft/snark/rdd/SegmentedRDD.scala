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

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import net.fnothaft.snark.SnarkContext._
import net.fnothaft.snark.{ ArrayStructure, NestedIndex }
import scala.annotation.tailrec
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

private[rdd] class SegmentedRDD[T](override protected val rdd: RDD[(NestedIndex, T)],
                                   override protected val structure: ArrayStructure,
                                   override protected val strategy: PartitioningStrategy.Strategy = PartitioningStrategy.Segmented) extends NestedRDD[T](rdd,
  structure,
  strategy) {

  /**
   * Maps a function to every element of this RDD.
   *
   * @param op A function to map to every element of this RDD.
   * @return A new nested RDD with element type U. The RDD will have the same structure
   * as the RDD the map is called on.
   *
   * @see mapWithIndex
   */
  override def map[U](op: T => U)(implicit uTag: ClassTag[U]): NestedRDD[U] = {
    new SegmentedRDD[U](rdd.mapPartitions(iter => {
      iter.map(kv => {
        val (idx, v) = kv

        (idx, op(v))
      })
    }, true), structure, strategy)
  }

  /**
   * Maps a function to every element of this RDD, along with the index of each element.
   *
   * @param op A function that maps the value of a point, as well as it's index, to a new value.
   * @return A new nested RDD with element type U. This RDD will have the same structure as
   * the RDD the map is called on. Additionally, each point will retain it's index.
   *
   * @see map
   */
  override def mapWithIndex[U](op: (T, NestedIndex) => U)(
    implicit uTag: ClassTag[U]): NestedRDD[U] = {
    new SegmentedRDD[U](rdd.mapPartitions(iter => {
      iter.map(kv => {
        val (idx, v) = kv

        (idx, op(v, idx))
      })
    }, true), structure, strategy)
  }

  /**
   * Applies a prefix scan over the RDD. The scan proceeds in order given by the
   * indices of all elements in the RDD.
   *
   * @param scanZero The zero value for the scan.
   * @param updateZero The zero value for the update pass.
   * @param scanOp The function to apply during the scan.
   * @param updateOp The function to apply during the update pass.
   * @return Returns a scanned RDD.
   */
  override def scan[U](scanZero: U,
                       updateZero: U)(scanOp: (U, T) => U,
                                      updateOp: (U, U) => U)(implicit tTag: ClassTag[T],
                                                             uTag: ClassTag[U]): NestedRDD[U] = {

    // create accumulator
    val accumMap = rdd.context.accumulableCollection(HashMap[Int, U]())

    // do the first scan pass
    val firstPass = rdd.mapPartitionsWithIndex((idx, iter) => {
      // scan the values in this nest
      val (rv, accumV) = doScan(scanOp, iter, scanZero)

      // add the accumulated value to the accumulator
      accumMap += (idx, accumV)

      // return the scanned values as an iterator
      rv.toIterator
    })

    // cache the first pass
    firstPass.cache()

    // force computation, so that we can guarantee accumulator access
    firstPass.count()

    // collect the propegated values
    val collectedPropegates = accumMap.value
      .toSeq
      .sortBy(kv => kv._1)
      .map(kv => kv._2)
      .toArray

    // do scan update...
    var runningValue = updateZero
    (0 until collectedPropegates.length).foreach(i => {
      val currentValue = collectedPropegates(i)

      // update in place
      collectedPropegates(i) = runningValue

      // calculate new running value
      runningValue = updateOp(runningValue, currentValue)
    })

    // map and do update
    val finalScanRDD = firstPass.map(kv => {
      val (idx, value) = kv

      // look up update
      val update = collectedPropegates(idx.nest)

      // update and return
      (idx, updateOp(value, update))
    })

    // unpersist cached rdd
    firstPass.unpersist()

    new SegmentedRDD[U](finalScanRDD, structure, strategy)
  }

  /**
   * Applies a reduce within each nested segment. This operates on all nested segments.
   *
   * @param op Reduction function to apply.
   * @return Returns a map, which maps each nested segment ID to the reduction value.
   */
  override def segmentedReduce(op: (T, T) => T)(implicit tTag: ClassTag[T]): Map[Int, T] = {
    rdd.mapPartitionsWithIndex((idx, iter) => {
      Iterator((idx, iter.map(kv => kv._2).reduce(op)))
    }).collect
      .toMap
  }

  /**
   * Performs a scan on all of the segments of this RDD, with a different zero value
   * per each segment.
   *
   * @param op Function to use for the scan.
   * @param zero Sequence of zero values to use for the scan.
   * @return New RDD where each segment has been operated on by a scan.
   */
  override def segmentedScan[U](zeros: Seq[U])(scanOp: (U, T) => U,
                                               updateOp: (U, U) => U)(
                                                 implicit uTag: ClassTag[U]): NestedRDD[U] = {
    assert(zeros.length == structure.nests,
      "Zeros must match to structure of RDD.")

    new SegmentedRDD[U](rdd.mapPartitionsWithIndex((idx, iter) => {
      val (res, _) = doScan(scanOp, iter, zeros(idx))

      // return iterator
      res.toIterator
    }), structure, strategy)
  }

  /**
   * Returns the value at a certain nested index.
   */
  override def get(idx: NestedIndex)(implicit tTag: ClassTag[T]): T = {
    @tailrec def checkAndTake(iter: Iterator[(NestedIndex, T)], depth: Int = 0): Iterator[T] = {
      if (!iter.hasNext) {
        throw new IllegalArgumentException("Value with index " + idx +
          " not found in nest " + idx.nest + ".")
      } else if (depth == idx.idx) {
        val foundValue = iter.next

        // check that we have found the correct value
        assert(foundValue._1 == idx, "Found value, but has incorrect index: " + idx)

        // return an iterator with just the value
        Iterator(foundValue._2)
      } else {
        checkAndTake(iter.drop(1), depth + 1)
      }
    }

    rdd.mapPartitionsWithIndex((nest, iter) => {
      if (nest == idx.nest) {
        checkAndTake(iter)
      } else {
        Iterator[T]()
      }
    }).first
  }
}

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
import scala.math.{ log, pow }
import scala.reflect.ClassTag

private[snark] class UniformRDD[T](override private[snark] val rdd: RDD[(NestedIndex, T)],
                                   override private[snark] val structure: ArrayStructure,
                                   override val strategy: PartitioningStrategy.Strategy = PartitioningStrategy.Uniform) extends NestedRDD[T](rdd,
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
    new UniformRDD[U](rdd.map(kv => {
      val (idx, v) = kv

      (idx, op(v))
    }), structure, strategy)
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
  override def mapWithIndex[U](op: (T, NestedIndex) => U)(implicit uTag: ClassTag[U]): NestedRDD[U] = {
    new UniformRDD[U](rdd.map(kv => {
      val (idx, v) = kv

      (idx, op(v, idx))
    }), structure, strategy)
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
    val finalScanRDD = firstPass.mapPartitionsWithIndex((pIdx, iter) => {
      // look up update
      val update = collectedPropegates(pIdx)

      iter.map(kv => {
        val (idx, value) = kv

        // update and return
        (idx, updateOp(value, update))
      })
    })

    // unpersist cached rdd
    firstPass.unpersist()

    new UniformRDD[U](finalScanRDD, structure, strategy)
  }

  private def segmentedReduceHelper(op: (T, T) => T)(implicit tTag: ClassTag[T]): RDD[(Int, T)] = {
    @tailrec def reducePartition(iter: Iterator[(NestedIndex, T)],
                                 currentNest: Int,
                                 currentValue: T,
                                 currentList: List[(Int, T)] = List()): Iterator[(Int, T)] = {
      if (!iter.hasNext) {
        ((currentNest, currentValue) :: currentList).toIterator
      } else {
        val (curIdx, curVal) = iter.next
        val (nextNest, nextValue, nextList) = if (curIdx.nest == currentNest) {
          (currentNest, op(currentValue, curVal), currentList)
        } else {
          (curIdx.nest, curVal, (currentNest, currentValue) :: currentList)
        }
        reducePartition(iter, nextNest, nextValue, nextList)
      }
    }

    rdd.mapPartitions(iter => {
      if (iter.hasNext) {
        val (idx, value) = iter.next
        reducePartition(iter, idx.nest, value)
      } else {
        Iterator[(Int, T)]()
      }
    })
  }

  /**
   * Applies a reduce within each nested segment. This operates on all nested segments.
   *
   * @param op Reduction function to apply.
   * @return Returns a map, which maps each nested segment ID to the reduction value.
   */
  override def segmentedReduce(op: (T, T) => T)(implicit tTag: ClassTag[T]): Map[Int, T] = {
    segmentedReduceHelper(op)
      .reduceByKeyLocally(op)
      .toMap
  }

  /**
   * Applies a reduce within each nested segment. This operates on all nested segments.
   *
   * @param op Reduction function to apply.
   * @return Returns a map, which maps each nested segment ID to the reduction value.
   */
  override def segmentedReduceToRdd(op: (T, T) => T)(implicit tTag: ClassTag[T]): RDD[(Int, T)] = {
    segmentedReduceHelper(op)
      .reduceByKey(op)
  }

  protected final def doScanInNest[U](scanOp: (U, T) => U,
                                      i: Iterator[(NestedIndex, T)],
                                      zeros: Seq[U]): (List[(NestedIndex, U)], (Int, U)) = {
    @tailrec def doScanInNestHelper(iter: Iterator[(NestedIndex, T)],
                                    runningNest: Int,
                                    runningValue: U,
                                    l: List[(NestedIndex, U)] = List()): (List[(NestedIndex, U)], (Int, U)) = {
      if (!iter.hasNext) {
        (l, (runningNest, runningValue))
      } else {
        val (currentIndex, value) = iter.next

        val (nextL, nextVal, nextNest): (List[(NestedIndex, U)], U, Int) = if (currentIndex.nest == runningNest) {
          ((currentIndex, runningValue) :: l, scanOp(runningValue, value), runningNest)
        } else {
          ((currentIndex, zeros(currentIndex.nest)) :: l,
            scanOp(zeros(currentIndex.nest), value),
            currentIndex.nest)
        }

        doScanInNestHelper(iter, nextNest, nextVal, nextL)
      }
    }

    if (i.hasNext) {
      val (currIdx, currVal) = i.next
      doScanInNestHelper(i,
        currIdx.nest,
        scanOp(zeros(currIdx.nest), currVal),
        List((currIdx, zeros(currIdx.nest))))
    } else {
      throw new IllegalArgumentException("Was passed empty iterator.")
    }
  }

  override def segmentedScan[U](zeros: Seq[U])(scanOp: (U, T) => U,
                                               updateOp: (U, U) => U)(implicit uTag: ClassTag[U]): NestedRDD[U] = {

    // create accumulator
    val accumMap = rdd.context.accumulableCollection(HashMap[Int, (Int, U)]())

    // do the first scan pass
    val firstPass = rdd.mapPartitionsWithIndex((idx, iter) => {
      // scan the values in this nest
      val (rv, accumV) = doScanInNest(scanOp, iter, zeros)

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
      .toArray

    // map and do update
    val finalScanRDD = firstPass.mapPartitionsWithIndex((pIdx, iter) => {
      // look up update
      val update = collectedPropegates.filter(kv => kv._1 < pIdx)
        .map(kv => kv._2)
        .groupBy(kv => kv._1)
        .map(kv => {
          val (nest, seq) = kv
          (nest, seq.map(kv => kv._2).reduce(updateOp))
        }).toMap

      iter.map(kv => {
        val (idx, value) = kv

        // update and return, if nest has previous values
        update.get(idx.nest).fold((idx, value))(updateVal => (idx, updateOp(value, updateVal)))
      })
    })

    // unpersist cached rdd
    firstPass.unpersist()

    new UniformRDD[U](finalScanRDD, structure, strategy)
  }
}

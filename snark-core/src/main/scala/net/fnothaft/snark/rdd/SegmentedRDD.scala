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

private[snark] class SegmentedRDD[T](override private[snark] val rdd: RDD[(NestedIndex, T)],
                                     override private[snark] val structure: ArrayStructure,
                                     override val strategy: PartitioningStrategy.Strategy = PartitioningStrategy.Segmented) extends NestedRDD[T](rdd,
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
  override def segmentedReduceToRdd(op: (T, T) => T)(implicit tTag: ClassTag[T]): RDD[(Int, T)] = {
    rdd.mapPartitionsWithIndex((idx, iter) => {
      if (iter.hasNext) {
        Iterator((idx, iter.map(kv => kv._2).reduce(op)))
      } else {
        Iterator[(Int, T)]()
      }
    })
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
   * Executes an elemental operation across two nested RDDs. The two nested RDDs must have the
   * same structure. In this operation, both elements at an index have a function applied to them.
   *
   * @param op Function to apply.
   * @param r Other nested RDD to perform P operation on. Must have the same structure as this RDD.
   */
  override def p[U, V](op: (T, U) => V)(r: NestedRDD[U])(implicit uTag: ClassTag[U], vTag: ClassTag[V]): NestedRDD[V] = {
    assert(structure.equals(r.structure),
      "Cannot do a p-operation on two nested arrays with different sizes: " +
        structure + ", " + r.structure)

    val srdd: SegmentedRDD[U] = r match {
      case s: SegmentedRDD[U] => s
      case _                  => r.matchPartitioning(this).asInstanceOf[SegmentedRDD[U]]
    }

    new SegmentedRDD[V](rdd.zipPartitions(srdd.rdd)(twiddle(_, _, op)), structure, strategy)
  }

  private def twiddle[U, V](iterT: Iterator[(NestedIndex, T)],
                            iterU: Iterator[(NestedIndex, U)],
                            op: (T, U) => V): Iterator[(NestedIndex, V)] = {
    @tailrec def twiddleHelper(iterT: Iterator[(NestedIndex, T)],
                               currT: (NestedIndex, T),
                               iterU: Iterator[(NestedIndex, U)],
                               currU: (NestedIndex, U),
                               l: List[(NestedIndex, V)]): Iterator[(NestedIndex, V)] = {
      if (!iterT.hasNext || !iterU.hasNext) {
        val finalL = if (currT._1.eq(currU._1)) {
          (currT._1, op(currT._2, currU._2)) :: l
        } else {
          l
        }
        finalL.reverse.toIterator
      } else {
        val (nextT, nextU, nextL) = if (currT._1.eq(currU._1)) {
          (iterT.next, iterU.next, (currT._1, op(currT._2, currU._2)) :: l)
        } else if (currT._1.lt(currU._1)) {
          (iterT.next, currU, l)
        } else {
          (currT, iterU.next, l)
        }

        // recurse
        twiddleHelper(iterT, nextT, iterU, nextU, nextL)
      }
    }

    if (iterT.hasNext && iterU.hasNext) {
      twiddleHelper(iterT, iterT.next, iterU, iterU.next, List())
    } else {
      Iterator()
    }
  }

  /**
   * Executes an elemental operation across two nested RDDs. The two nested RDDs must have the
   * same structure. In this operation, both elements at an index have a function applied to them.
   *
   * @param op Function to apply.
   * @param r Other nested RDD to perform P operation on. Must have the same structure as this RDD.
   */
  override def p(op: (T, T) => T, preserveUnpaired: Boolean)(r: NestedRDD[T])(implicit tTag: ClassTag[T]): NestedRDD[T] = {

    assert(structure.equals(r.structure),
      "Cannot do a p-operation on two nested arrays with different sizes: " +
        structure + ", " + r.structure)

    val srdd: SegmentedRDD[T] = r match {
      case s: SegmentedRDD[T] => s
      case _                  => r.matchPartitioning(this).asInstanceOf[SegmentedRDD[T]]
    }

    new SegmentedRDD[T](rdd.zipPartitions(srdd.rdd)(twiddle(_, _, op, preserveUnpaired)), structure, strategy)
  }

  private def twiddle(iterT1: Iterator[(NestedIndex, T)],
                      iterT2: Iterator[(NestedIndex, T)],
                      op: (T, T) => T,
                      preserveUnpaired: Boolean): Iterator[(NestedIndex, T)] = {
    @tailrec def twiddleHelper(iterT1: Iterator[(NestedIndex, T)],
                               currT1: (NestedIndex, T),
                               iterT2: Iterator[(NestedIndex, T)],
                               currT2: (NestedIndex, T),
                               l: List[(NestedIndex, T)]): Iterator[(NestedIndex, T)] = {
      if (!iterT1.hasNext && !iterT2.hasNext) {
        val finalL = if (currT1._1.eq(currT2._1)) {
          (currT1._1, op(currT1._2, currT2._2)) :: l
        } else if (preserveUnpaired) {
          if (currT1._1.lt(currT2._1)) {
            (currT2 :: (currT1 :: l))
          } else {
            (currT1 :: (currT2 :: l))
          }
        } else {
          l
        }
        finalL.reverse.toIterator
      } else {
        val (nextT1, nextT2, nextL) = if (currT1._1.eq(currT2._1)) {
          val ni1 = if (iterT1.hasNext) {
            iterT1.next
          } else {
            currT1
          }
          val ni2 = if (iterT2.hasNext) {
            iterT2.next
          } else {
            currT2
          }
          (ni1, ni2, (currT1._1, op(currT1._2, currT2._2)) :: l)
        } else if (currT1._1.lt(currT2._1)) {
          val ni = if (iterT1.hasNext) {
            iterT1.next
          } else {
            currT1
          }
          if (preserveUnpaired) {
            (ni, currT2, currT1 :: l)
          } else {
            (ni, currT2, l)
          }
        } else {
          val ni = if (iterT2.hasNext) {
            iterT2.next
          } else {
            currT2
          }
          if (preserveUnpaired) {
            (currT1, ni, currT2 :: l)
          } else {
            (currT1, ni, l)
          }
        }

        // recurse
        twiddleHelper(iterT1, nextT1, iterT2, nextT2, nextL)
      }
    }

    val l1 = iterT1.toList
    val l2 = iterT2.toList

    val _iterT1 = l1.toIterator
    val _iterT2 = l2.toIterator

    if (_iterT1.hasNext && _iterT2.hasNext) {
      twiddleHelper(_iterT1, _iterT1.next, _iterT2, _iterT2.next, List())
    } else if (preserveUnpaired) {
      _iterT1 ++ _iterT2
    } else {
      Iterator()
    }
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

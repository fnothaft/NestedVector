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

import org.apache.spark.rdd.RDD
import net.fnothaft.snark.{ NestedIndex, ArrayStructure }
import net.fnothaft.snark.util.SparkFunSuite

class NestedRDDSuite extends SparkFunSuite {

  sparkTest("build a simple index") {
    val index = NestedRDD.index(sc, 10)

    assert(index.count === 10)
    assert(index.filter(_.nest == 0).count === 10)
    (0 until 10).foreach(i => {
      assert(index.filter(_.idx == i).count === 1)
    })
  }

  sparkTest("can build a simple nested array") {
    val index = NestedRDD.index(sc, 10).coalesce(1)
    val rdd = sc.parallelize(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), 1)
    val structure = ArrayStructure(Seq(10L))
    val zipRdd = index.zip(rdd)

    val nRdd = NestedRDD(zipRdd, structure, PartitioningStrategy.None)

    assert(nRdd.countWithSideEffects === 10)
    nRdd.collect.foreach(kv => assert(kv._1.idx === kv._2))
  }

  sparkTest("perform simple scan: factorial") {
    val index = NestedRDD.index(sc, 5).coalesce(1)
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5), 1)
    val structure = ArrayStructure(Seq(5L))
    val zipRdd = index.zip(rdd)

    val nRdd = NestedRDD(zipRdd, structure, PartitioningStrategy.None)

    assert(nRdd.countWithSideEffects === 5)

    val sRdd = nRdd.segmentedScan(Seq(1))(_ * _)
    assert(sRdd.countWithSideEffects === 5)

    def factorial(n: Int): Int = if (n > 1) {
      (1 to n).reduce(_ * _)
    } else {
      1
    }

    sRdd.collect.foreach(kv => {
      assert(factorial(kv._1.idx) === kv._2)
    })
  }

  sparkTest("perform simple scan: sum") {
    val index = NestedRDD.index(sc, 5).coalesce(1)
    val rdd = sc.parallelize(Seq(1, 1, 1, 1, 1), 1)
    val structure = ArrayStructure(Seq(5L))
    val zipRdd = index.zip(rdd)

    val nRdd = NestedRDD(zipRdd, structure, PartitioningStrategy.None)

    assert(nRdd.countWithSideEffects === 5)

    val sRdd = nRdd.segmentedScan(Seq(1))(_ + _)
    assert(sRdd.countWithSideEffects === 5)

    sRdd.collect.foreach(kv => {
      assert((kv._1.idx + 1) === kv._2)
    })
  }

  sparkTest("perform simple reduce") {
    val index = NestedRDD.index(sc, 5).coalesce(1)
    val rdd = sc.parallelize(Seq(1, 1, 1, 1, 1), 1)
    val structure = ArrayStructure(Seq(5L))
    val zipRdd = index.zip(rdd)

    val nRdd = NestedRDD(zipRdd, structure, PartitioningStrategy.None)

    assert(nRdd.countWithSideEffects === 5)

    val r = nRdd.reduce(_ + _)
    val sr = nRdd.segmentedReduce(_ + _)

    assert(sr.size === 1)
    assert(r === 5)
    assert(sr(0) === r)
  }

  sparkTest("perform simple p operand operation") {
    val index = NestedRDD.index(sc, 5).coalesce(1)
    val rdd1 = sc.parallelize(Seq(1, 1, 1, 1, 1), 1)
    val rdd2 = sc.parallelize(Seq(-1, 0, 1, 2, 3), 1)
    val structure = ArrayStructure(Seq(5L))
    val zipRdd1 = index.zip(rdd1)
    val zipRdd2 = index.zip(rdd2)

    val nRdd1 = NestedRDD(zipRdd1, structure, PartitioningStrategy.None)
    val nRdd2 = NestedRDD(zipRdd2, structure, PartitioningStrategy.None)

    assert(nRdd1.countWithSideEffects === 5)
    assert(nRdd2.countWithSideEffects === 5)

    val pRdd = nRdd1.p[Int, Int](_ + _)(nRdd2)

    assert(pRdd.countWithSideEffects === 5)
    pRdd.collect.foreach(kv => assert(kv._1.idx === kv._2))
  }

  sparkTest("perform a simple scan") {
    val index = NestedRDD.index(sc, 10).coalesce(1)
    val rdd = sc.parallelize(Seq(1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 1)
    val structure = ArrayStructure(Seq(10L))

    val nRdd = NestedRDD(index.zip(rdd), structure, PartitioningStrategy.None)

    val scanRdd = nRdd.scan(0, 0)(_ + _, _ + _)

    assert(scanRdd.countWithSideEffects === 10)
    assert(scanRdd.toRDD.distinct.count === 10)
    assert(scanRdd.toRDD.collect.toSeq.min === 0)
    assert(scanRdd.toRDD.collect.toSeq.max === 9)
  }

  sparkTest("perform a more complex scan") {
    val index = NestedRDD.index(sc, 1000).coalesce(5, true)
    val rdd = sc.parallelize((0 until 1000).toList.map(i => 1)).coalesce(5, true)
    val structure = ArrayStructure(Seq(1000L))

    val nRdd = NestedRDD(index.zip(rdd), structure, PartitioningStrategy.None)

    val scanRdd = nRdd.scan(0, 0)(_ + _, _ + _)

    assert(scanRdd.countWithSideEffects === 1000)
    assert(scanRdd.toRDD.distinct.count === 1000)
    assert(scanRdd.toRDD.collect.toSeq.min === 0)
    assert(scanRdd.toRDD.collect.toSeq.max === 999)
  }
}

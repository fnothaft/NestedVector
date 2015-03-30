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
package net.fnothaft.snark.linalg

import net.fnothaft.snark.{ DenseArrayStructure, NestedIndex, SparseArrayStructure }
import net.fnothaft.snark.rdd.{ NestedRDD, PartitioningStrategy }
import net.fnothaft.snark.util.SparkFunSuite
import org.apache.spark.mllib.linalg.{ DenseVector, SparseVector }

class DenseMatrixSuite extends SparkFunSuite {

  sparkTest("test by multiplying versus identity scalar") {
    val rdd = sc.parallelize(Seq((NestedIndex(0, 0), 1.3), (NestedIndex(0, 1), 1.5),
      (NestedIndex(1, 0), 2.5), (NestedIndex(1, 1), -3.0)))
    val structure = new DenseArrayStructure(Seq(2L, 2L))
    val nRdd = NestedRDD(rdd, structure, PartitioningStrategy.Segmented)
    val matrix = new DenseMatrix(nRdd, DenseMatrixStructure(structure))

    val nMatrix = matrix.scalarMultiply(-1.0).matrixAdd(matrix)

    assert(nMatrix.asInstanceOf[DenseMatrix]
      .rdd
      .map(v => (v < 1e-3 && v > -1e-3))
      .reduce(_ && _))
  }

  sparkTest("add a sparse and dense matrix") {
    val rddD = sc.parallelize(Seq((NestedIndex(0, 0), 1.0), (NestedIndex(0, 1), 1.5),
      (NestedIndex(1, 0), 2.5), (NestedIndex(1, 1), 1.0)))
    val structureD = new DenseArrayStructure(Seq(2L, 2L))
    val nRddD = NestedRDD(rddD, structureD, PartitioningStrategy.Segmented)
    val matrixD = new DenseMatrix(nRddD, DenseMatrixStructure(structureD))

    val rddS = sc.parallelize(Seq((NestedIndex(0, 1), -1.5), (NestedIndex(1, 0), -2.5)))
    val structureS = new SparseArrayStructure(Map((0 -> 2L), (1 -> 2L)))
    val nRddS = NestedRDD(rddS, structureS, PartitioningStrategy.Segmented)
    val matrixS = new SparseMatrix(nRddS, SparseMatrixStructure(2, 2))

    val nMatrix = matrixD.matrixAdd(matrixS)

    assert(nMatrix.asInstanceOf[DenseMatrix]
      .rdd
      .mapWithIndex((v, idx) => if (idx.idx == idx.nest) {
        v < 1.0001 && v > 0.9999
      } else {
        (v < 1e-3 && v > -1e-3)
      }).reduce(_ && _))
  }

  sparkTest("test matrix matrix multiply on identity matrix") {
    val rdd = sc.parallelize(Seq((NestedIndex(0, 0), 1.0), (NestedIndex(0, 1), 0.0),
      (NestedIndex(1, 0), 0.0), (NestedIndex(1, 1), 1.0)))
    val structure = new DenseArrayStructure(Seq(2L, 2L))
    val nRdd = NestedRDD(rdd, structure, PartitioningStrategy.Segmented)
    val matrix = new DenseMatrix(nRdd, DenseMatrixStructure(structure))

    val nMatrix = matrix.matrixMultiply(matrix)

    assert(nMatrix.asInstanceOf[DenseMatrix].rdd
      .mapWithIndex((v, idx) => {
        if (idx.nest == idx.idx) {
          v == 1.0
        } else {
          v == 0.0
        }
      }).reduce(_ && _))
  }

  sparkTest("multiply matrix by dense vector") {
    val rdd = sc.parallelize(Seq((NestedIndex(0, 0), 1.0), (NestedIndex(0, 1), 0.0),
      (NestedIndex(1, 0), 0.0), (NestedIndex(1, 1), 1.0)))
    val structure = new DenseArrayStructure(Seq(2L, 2L))
    val nRdd = NestedRDD(rdd, structure, PartitioningStrategy.Segmented)
    val matrix = new DenseMatrix(nRdd, DenseMatrixStructure(structure))

    val dv = new DenseVector(Array(1.0, 0.0))

    val cv = matrix.vectorMultiply(dv).asInstanceOf[DenseVector]

    assert(cv.size === 2)
    assert(cv(0) === 1.0)
    assert(cv(1) === 0.0)
  }

  sparkTest("multiply matrix by sparse vector") {
    val rdd = sc.parallelize(Seq((NestedIndex(0, 0), 1.0), (NestedIndex(0, 1), 0.0),
      (NestedIndex(1, 0), 0.0), (NestedIndex(1, 1), 1.0)))
    val structure = new DenseArrayStructure(Seq(2L, 2L))
    val nRdd = NestedRDD(rdd, structure, PartitioningStrategy.Segmented)
    val matrix = new DenseMatrix(nRdd, DenseMatrixStructure(structure))

    val sv = new SparseVector(2, Array(1), Array(1.0))

    val cv = matrix.vectorMultiply(sv).asInstanceOf[DenseVector]

    assert(cv.size === 2)
    assert(cv(0) === 0.0)
    assert(cv(1) === 1.0)
  }
}

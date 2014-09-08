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

import net.fnothaft.snark.NestedIndex
import net.fnothaft.snark.rdd.NestedRDD
import org.apache.spark.mllib.linalg.{
  DenseVector,
  SparseVector,
  Vector => SparkVector
}

private[linalg] class SparseMatrix(private[linalg] val rdd: NestedRDD[Double],
                                   override private[linalg] val structure: SparseMatrixStructure) extends Matrix(structure) {

  def matrixAdd(mat: Matrix): Matrix = mat match {
    case dm: DenseMatrix => dm.matrixAdd(this)
    case sm: SparseMatrix => {
      if (structure.h == sm.structure.h && structure.w == sm.structure.w) {
        new SparseMatrix(rdd.matchPartitioning(sm.rdd).p(_ + _, true)(sm.rdd), structure)
      } else {
        throw new IllegalArgumentException("Matrices must have same dimension.")
      }
    }
  }

  def scalarMultiply(scalar: Double): Matrix = {
    new SparseMatrix(rdd.map(_ * scalar), structure)
  }

  def matrixMultiply(mat: Matrix): Matrix = {
    if (canMultiply(mat)) {
      // new height and width
      val newH = structure.h
      val newW = mat.structure.w

      mat match {
        case dm: DenseMatrix => this.matrixMultiply(dm.toSparse)
        case sm: SparseMatrix => {
          val newStructure = SparseMatrixStructure(newH, newW)

          val flatLeftMatrix = rdd.multiScatter(kv => {
            val (idx, v) = kv
            (0 until newH).map(i => {
              (NestedIndex(idx.nest * newH + i, idx.idx), v)
            }).toIterable
          })((v1: Double, v2: Double) => v1 + v2)

          val flatRightMatrix = sm.rdd.multiScatter(kv => {
            val (idx, v) = kv
            (0 until newW).map(i => {
              (NestedIndex(idx.idx + i * newW, idx.nest), v)
            }).toIterable
          })((v1: Double, v2: Double) => v1 + v2)

          new SparseMatrix(NestedRDD[Double](flatLeftMatrix.p((v1: Double, v2: Double) => v1 * v2, false)(
            flatRightMatrix).segmentedReduceToRdd(_ + _)
            .map(kv => {
              val (i, v) = kv

              val nest = i / newH
              val idx = i % newH

              (NestedIndex(nest, idx), v)
            }), newStructure.toStructure, rdd.strategy),
            newStructure)
        }
        case _ => throw new IllegalArgumentException("Unknown matrix type.")
      }
    } else {
      throw new IllegalArgumentException("Matrix dimensions cannot be multiplied: (" +
        structure.w + " * " + structure.h + ") x (" +
        mat.structure.w + " * " + structure.h + ")")
    }
  }

  def vectorMultiply(vec: SparkVector): SparkVector = {
    val vMap = vec match {
      case dv: DenseVector => {
        assert(dv.size == structure.w, "Matrix width (" + structure.w +
          ") doesn't equal vector length (" + dv.size + ").")
        rdd.mapWithIndex((v, idx) => v * dv(idx.idx))
          .segmentedReduce(_ + _)
      }
      case sv: SparseVector => {
        assert(sv.size == structure.w, "Matrix width (" + structure.w +
          ") doesn't equal vector length (" + sv.size + ").")
        rdd.mapWithIndex((v, idx) => {
          val vIdx = sv.indices.indexOf(idx.idx)
          if (vIdx == -1) {
            0.0
          } else {
            v * sv.values(vIdx)
          }
        }).segmentedReduce(_ + _)
      }
      case _ => throw new IllegalArgumentException("Unexpected vector type.")
    }
    new SparseVector(structure.w, vMap.keys.toArray, vMap.values.toArray)
  }
}

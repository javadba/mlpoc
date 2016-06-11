package com.blazedb.ml

import com.blazedb.ml.LocalLinalg.Vertices
import org.apache.spark.SparkContext
import com.blazedb.ml.{RDDLinalg => RDDLA, LocalLinalg => LA}
import org.apache.spark.mllib.linalg.{BreezeHelpers =>BH}
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseVector => BDV}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * ClusteringUtils
 *
 */
object ClusteringUtils {


  type DVector = Array[Double]
  type DMatrix = Array[DVector]

  type LabeledVector = (String, DVector)

  type IndexedVector = (Int, DVector)

//  type Vertices = Seq[LabeledVector]

  val DefaultMinNormAccel: Double = 1e-11

  val DefaultIterations: Int = 20

   def cluster(sc: SparkContext, vertices: Vertices, nClusters: Int, sigma: Double,
    nPowerIterations: Int) = {
    val nVertices = vertices.length
    val gaussRdd = createGaussianRdd(sc, vertices, sigma).cache()

    var ix = 0
    val indexedGaussRdd = gaussRdd.map { d =>
      ix += 1
      (ix, d)
    }

    val (columnsRdd, colSumsRdd) = RDDLA.createColumnsRdds(sc, vertices, indexedGaussRdd)
    val indexedDegreesRdd = createLaplacianRdd(sc, vertices, indexedGaussRdd, colSumsRdd)
    RDDLA.eigens(sc, indexedDegreesRdd.map{case (x,dv) => (x.toLong,BH.toBreeze(dv))},
     nClusters, nPowerIterations)
  }

  def readVerticesfromFile(verticesFile: String): Vertices = {

    import scala.io.Source
    val vertices = Source.fromFile(verticesFile).getLines.map { l =>
      val toks = l.split("\t")
      val arr = new BDV(toks.slice(1, toks.length).map(_.toDouble))
      (toks(0).toLong, arr)
    }.toList
    println(s"Read in ${vertices.length} from $verticesFile")
    vertices
  }

  def gaussianDist(c1arr: DVector, c2arr: DVector, sigma: Double) = {
    val c1c2 = c1arr.zip(c2arr)
    val dist = Math.exp((0.5 / Math.pow(sigma, 2.0)) * c1c2.foldLeft(0.0) {
      case (dist: Double, (c1: Double, c2: Double)) =>
        dist - Math.pow(c1 - c2, 2)
    })
    dist
  }

  def createGaussianRdd(sc: SparkContext, vertices: Vertices, sigma: Double) = {
    val nVertices = vertices.length
    val gaussRdd = sc.parallelize({
      val dvect = new Array[DVector](nVertices)
      for (i <- vertices.indices) {
        dvect(i) = new DVector(nVertices)
        for (j <- vertices.indices) {
          dvect(i)(j) = if (i != j) {
            gaussianDist(vertices(i)._2.toArray, vertices(j)._2.toArray, sigma)
          } else {
            0.0
          }
        }
      }
      dvect
    }, nVertices)
    gaussRdd
  }

  def createLaplacianRdd(sc: SparkContext,
    vertices: Vertices,
    indexedDegreesRdd: RDD[IndexedVector],
    colSums: RDD[(Int, Double)]) = {
    val nVertices = vertices.length
    val bcNumVertices = sc.broadcast(nVertices)
    val bcColSums = sc.broadcast(colSums.collect)
    val laplaceRdd = indexedDegreesRdd.mapPartitionsWithIndex({ (partIndex, iter) =>
      val localNumVertices = bcNumVertices.value
      val localColSums = bcColSums.value
      var rowctr = -1
      iter.toList.map { case (dindex, dval) =>
        for (ix <- 0 until dval.length) {
          dval(ix) = (1.0 / localColSums(partIndex)._2) *
            (if (ix != partIndex) {
              -1.0 * dval(ix)
            } else {
              1.0
            }
              )
        }
        (rowctr, dval)
      }.iterator
    }, preservesPartitioning = true)

    laplaceRdd
  }


}

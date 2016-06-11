package com.blazedb.ml.util

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

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.random.RandomRDDs._

/**
 * DataGenerator
 *
 */
object DataGenerator {


  // Generate a random double RDD that contains 1 million i.i.d. values drawn from the
  // standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
  // Then apply a transform to get a random double RDD following `N(1, 4)`.
  def normal(sc: SparkContext, N: Long) = normalRDD(sc, N, 10).map {
    x =>
      1.0 + 2.0 * x
  }

  import org.apache.spark.mllib.stat._
  import org.apache.spark.rdd.RDD

  def getSummary(rdd: RDD[Vector]): MultivariateStatisticalSummary = {
    // Coming in 1.2.0 !  rdd.treeAggregate(new MultivariateOnlineSummarizer)(
    rdd.aggregate(new MultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))
  }

  def printSummary(summ: MultivariateStatisticalSummary) = {
    println(s"Count: ${summ.count}\nMean: ${summ.mean}\nMax: ${summ.max}\nVar: ${summ.variance}")
  }

  def genRandomRdd(sc: SparkContext, n: Int) = {
    import java.util.Random
    val xrand = new Random(37)
    val yrand = new Random(41)
    val zrand = new Random(43)
    val SignalScale = 5.0
    val NoiseScale = 8.0
    val npoints = n
    val XScale = SignalScale / npoints
    val YScale = 2.0 * SignalScale / npoints
    val ZScale = 3.0 * SignalScale / npoints
    val randRdd = sc.parallelize({
      for (p <- Range(0, npoints))
        yield
        (NoiseScale * xrand.nextGaussian + XScale * p,
          NoiseScale * yrand.nextGaussian + YScale * p,
          NoiseScale * zrand.nextGaussian + ZScale * p)
    }.toSeq, 2)
    val vecs = randRdd.map {
      case (x, y, z) =>
        Vectors.dense(Array(x, y, z))
    }
    val summary = getSummary(vecs)
    printSummary(summary)
    vecs
  }

  def pr(msg: String) = println(s"[${new java.util.Date()}] $msg")

  def main(args: Array[String]) {
    val sc = new TestingSparkContext("local[1]")
    val rrdd = genRandomRdd(sc.sc, 100)
    pr(s"rrdd is ${rrdd.collect.mkString(",")}")
  }
}





package scala.com.blazedb.ml

import com.blazedb.ml.ClusteringUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.SpectralClustering

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

import ClusteringUtils._
/**
 * TestSpectralClustering
 *
 */
object TestSpectralClustering {

    def main(args: Array[String]) {
    val sc = new SparkContext("local[1]", "TestSpark")
    val vertFile = "../data/graphx/new_lr_data.10.txt"
    val sigma = 1.0
    val nIterations = 3
    val nClusters = 3
    val vertices = readVerticesfromFile(vertFile)
    cluster(sc, vertices, nClusters, sigma, nIterations)
  }

}

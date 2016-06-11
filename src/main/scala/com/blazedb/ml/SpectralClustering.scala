package org.apache.spark.mllib.clustering

import com.blazedb.ml.{RDDLinalg, LocalLinalg}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseVector => BDV}

/**
 *
 * An alternate implementation of Power Iteration Clustering vs the spark.mllib one. The primary differences:
 *
 * (a) This implementation supports dense arrays of max  2billion dimensions. Practical limitations would dictate
 *     more on the order of 10^7 dimensions as a realistic maximum.
 *     spark.mllib supports infinitely dimensioned but highly sparse arrays
 * (b) This implementation uses vanilla RDD's and Breeze on the backend. It is optimized for dense matrices. spark.mllib
 *     uses graphx that operates on individual vertices and thus is orders of magnitude slower for each product calculation.
 * (c) This implementation uses Schur complement to provide a configurable "K" number of orthogonal eigenvectors.
 *      The spark.mllib implementation presently only supports a single eigenvector.
 *
 * More information on the theory of the algorithm:
 *
 * Implements the scalable graph clustering algorithm Power Iteration Clustering (see
 * www.icml2010.org/papers/387.pdf)
 * * From the abstract:
 *
 * The input data is first transformed to a normalized Affinity Matrix via Gaussian pairwise
 * distance calculations. Power iteration is then used to find a dimensionality-reduced
 * representation.  The resulting pseudo-eigenvector provides effective clustering - as
 * performed by Parallel KMeans.
 */
object SpectralClustering {

  private val logger = Logger.getLogger(getClass.getName())

  type DEdge = Edge[Double]
  type LabeledPoint = (VertexId, BDV[Double])
  type Points = Seq[LabeledPoint]
  type DGraph = Graph[Double, Double]
  type IndexedVector[Double] = (Long, BDV[Double])

  // Terminate iteration when norm changes by less than this value
  val DefaultMinNormChange: Double = 1e-11

  // Default σ for Gaussian Distance calculations
  val DefaultSigma = 1.0

  // Default number of iterations for PIC loop
  val DefaultIterations: Int = 20

  // Default minimum affinity between points - lower than this it is considered
  // zero and no edge will be created
  val DefaultMinAffinity = 1e-11

  val LA = LocalLinalg
  val RDDLA = RDDLinalg

  /**
   *
   * Run a Power Iteration Clustering
   *
   * @param sc  Spark Context
   * @param points  Input Points in format of [(VertexId,(x,y)]
   *                where VertexId is a Long
   * @param nClusters  Number of clusters to create
   * @param nIterations Number of iterations of the PIC algorithm
   *                    that calculates primary PseudoEigenvector and Eigenvalue
   * @param sigma   Sigma for Gaussian distribution calculation according to
   *                [1/2 *sqrt(pi*sigma)] exp (- (x-y)**2 / 2sigma**2
   * @param minAffinity  Minimum Affinity between two Points in the input dataset: below
   *                     this threshold the affinity will be considered "close to" zero and
   *                     no Edge will be created between those Points in the sparse matrix
   * @return Tuple of (Seq[(Cluster Id,Cluster Center)],
   *         Seq[(VertexId, ClusterID Membership)]
   */
  def run(sc: SparkContext,
          points: Points,
          nClusters: Int,
          nIterations: Int = DefaultIterations,
          sigma: Double = DefaultSigma,
          minAffinity: Double = DefaultMinAffinity)
  : (Seq[(Int, Vector)], Seq[((VertexId, Vector), Int)]) = {
    val vidsRdd = sc.parallelize(points.map(_._1).sorted)
    val nVertices = points.length

    val (wRdd, rowSums) = createNormalizedAffinityMatrix(sc, points, sigma)
    val initialVt = createInitialVector(sc, points.map(_._1), rowSums)
    if (logger.isDebugEnabled) {
      logger.debug(s"Vt(0)=${
        LA.printVector(new BDV(initialVt.map {
          _._2
        }.toArray))
      }")
    }
    val edgesRdd = createSparseEdgesRdd(sc, wRdd, minAffinity)
    val G = createGraphFromEdges(sc, edgesRdd, points.size, Some(initialVt))
    if (logger.isDebugEnabled) {
      logger.debug(RDDLA.printMatrixFromEdges(G.edges))
    }
    val (gUpdated, lambda, vt) = getPrincipalEigen(sc, G, nIterations)
    // TODO: avoid local collect and then sc.parallelize.
    val localVt = vt.collect.sortBy(_._1)
    val vectRdd = sc.parallelize(localVt.map(v => (v._1, Vectors.dense(v._2))))
    // TODO: what to set nRuns
    val nRuns = 10
    vectRdd.cache()
    val model = KMeans.train(vectRdd.map {
      _._2
    }, nClusters, nRuns)
    vectRdd.unpersist()
    if (logger.isDebugEnabled) {
      logger.debug(s"Eigenvalue = $lambda EigenVector: ${localVt.mkString(",")}")
    }
    val estimates = vectRdd.zip(model.predict(vectRdd.map(_._2)))
    if (logger.isDebugEnabled) {
      logger.debug(s"lambda=$lambda  eigen=${localVt.mkString(",")}")
    }
    val ccs = (0 until model.clusterCenters.length).zip(model.clusterCenters)
    if (logger.isDebugEnabled) {
      logger.debug(s"Kmeans model cluster centers: ${ccs.mkString(",")}")
    }
    val estCollected = estimates.collect.sortBy(_._1._1)
    if (logger.isDebugEnabled) {
      val clusters = estCollected.map(_._2)
      val counts = estCollected.groupBy(_._2).mapValues {
        _.length
      }
      logger.debug(s"Cluster counts: Counts: ${counts.mkString(",")}"
        + s"\nCluster Estimates: ${estCollected.mkString(",")}")
    }
    (ccs, estCollected)
  }

  /**
   * Creates an initial Vt(0) used within the first iteration of the PIC
   */
  def createInitialVector(sc: SparkContext,
                          labels: Seq[VertexId],
                          rowSums: Seq[Double]) = {
    val volume = rowSums.fold(0.0) {
      _ + _
    }
    val initialVt = labels.zip(rowSums.map(_ / volume))
    initialVt
  }

  /**
   * Create a Graph given an initial Vt0 and a set of Edges that
   * represent the Normalized Affinity Matrix (W)
   */
  def createGraphFromEdges(sc: SparkContext,
                           edgesRdd: RDD[DEdge],
                           nPoints: Int,
                           optInitialVt: Option[Seq[(VertexId, Double)]] = None) = {

    assert(nPoints > 0, "Must provide number of points from the original dataset")
    val G = if (optInitialVt.isDefined) {
      val initialVt = optInitialVt.get
      val vertsRdd = sc.parallelize(initialVt)
      Graph(vertsRdd, edgesRdd)
    } else {
      Graph.fromEdges(edgesRdd, -1.0)
    }
    G

  }

  /**
   * Calculate the dominant Eigenvalue and Eigenvector for a given sparse graph
   * using the PIC method
   * @param sc
   * @param G  Input Graph representing the Normalized Affinity Matrix (W)
   * @param nIterations Number of iterations of the PIC algorithm
   * @param optMinNormChange Minimum norm acceleration for detecting convergence
   *                         - indicated as "epsilon" in the PIC paper
   * @return
   */
  def getPrincipalEigen(sc: SparkContext,
                        G: DGraph,
                        nIterations: Int = DefaultIterations,
                        optMinNormChange: Option[Double] = None
                         ): (DGraph, Double, VertexRDD[Double]) = {

    var priorNorm = Double.MaxValue
    var norm = Double.MaxValue
    var priorNormVelocity = Double.MaxValue
    var normVelocity = Double.MaxValue
    var normAccel = Double.MaxValue
    val DummyVertexId = -99L
    var vnorm: Double = -1.0
    var outG: DGraph = null
    var prevG: DGraph = G
    val epsilon = optMinNormChange
      .getOrElse(1e-5 / G.vertices.count())
    for (iter <- 0 until nIterations
         if Math.abs(normAccel) > epsilon) {

      val tmpEigen = prevG.aggregateMessages[Double](ctx => {
        ctx.sendToSrc(ctx.attr * ctx.srcAttr);
        ctx.sendToDst(ctx.attr * ctx.dstAttr)
      },
        _ + _)
      if (logger.isDebugEnabled) {
        logger.debug(s"tmpEigen[$iter]: ${tmpEigen.collect.mkString(",")}\n")
      }
      val vnorm =
        prevG.vertices.map {
          _._2
        }.fold(0.0) { case (sum, dval) =>
          sum + Math.abs(dval)
        }
      if (logger.isDebugEnabled) {
        logger.debug(s"vnorm[$iter]=$vnorm")
      }
      outG = prevG.outerJoinVertices(tmpEigen) { case (vid, wval, optTmpEigJ) =>
        val normedEig = optTmpEigJ.getOrElse {
          -1.0
        } / vnorm
        if (logger.isDebugEnabled) {
          logger.debug(s"Updating vertex[$vid] from $wval to $normedEig")
        }
        normedEig
      }
      prevG = outG

      if (logger.isDebugEnabled) {
        val localVertices = outG.vertices.collect
        val graphSize = localVertices.size
        print(s"Vertices[$iter]: ${localVertices.mkString(",")}\n")
      }
      normVelocity = vnorm - priorNorm
      normAccel = normVelocity - priorNormVelocity
      if (logger.isDebugEnabled) {
        logger.debug(s"normAccel[$iter]= $normAccel")
      }
      priorNorm = vnorm
      priorNormVelocity = vnorm - priorNorm
    }
    (outG, vnorm, outG.vertices)
  }

  /**
   * Read Points from an input file in the following format:
   *   Vertex1Id Coord11 Coord12 CoordX13 .. Coord1D
   *   Vertex2Id Coord21 Coord22 CoordX23 .. Coord2D
   *    ..
   *   VertexNId CoordN1 CoordN2 CoordN23 .. CoordND
   *
   * Where N is the number of observations, each a D-dimension point
   *
   * E.g.
   *
   *   19	1.8035177495	0.7460582552	0.2361611395	-0.8645567427	-0.8613062
   *   10	0.5534111111	1.0456386879	1.7045663273	0.7281759816	1.0807487792
   *   911	1.200749626	1.8962364439	2.5117192131	-0.4034737281	-0.9069696484
   *   
   * Which represents three 5-dimensional input Points with VertexIds 19,10, and 911
   * @param verticesFile Local filesystem path to the Points input file
   * @return Set of Vertices in format appropriate for consumption by the PIC algorithm
   */
  def readVerticesfromFile(verticesFile: String): Points = {

    import scala.io.Source
    val vertices = Source.fromFile(verticesFile).getLines.map { l =>
      val toks = l.split("\t")
      val arr = new BDV(toks.slice(1, toks.length).map(_.toDouble))
      (toks(0).toLong, arr)
    }.toSeq
    if (logger.isDebugEnabled) {
      logger.debug(s"Read in ${vertices.length} from $verticesFile")
    }
    vertices
  }

  /**
   * Calculate the Gaussian distance between two Vectors according to:
   *
   *  exp( -(X1-X2)^2/2*sigma^2))
   *
   *  where X1 and X2 are Vectors
   *
   * @param vect1 Input Vector1
   * @param vect2 Input Vector2
   * @param sigma Gaussian parameter sigma
   * @return
   */
  def gaussianDist(vect1: BDV[Double], vect2: BDV[Double], sigma: Double) = {
    val c1c2 = vect1.toArray.zip(vect2.toArray)
    val dist = Math.exp((-0.5 / Math.pow(sigma, 2.0)) * c1c2.foldLeft(0.0) {
      case (dist: Double, (c1: Double, c2: Double)) =>
        dist + Math.pow(c1 - c2, 2)
    })
    dist
  }

  /**
   * Create a sparse EdgeRDD from an array of densevectors. The elements that
   * are "close to" zero - as configured by the minAffinity value - do not
   * result in an Edge being created.
   *
   * @param sc
   * @param wRdd
   * @param minAffinity
   * @return
   */
  def createSparseEdgesRdd(sc: SparkContext, wRdd: RDD[IndexedVector[Double]],
                           minAffinity: Double = DefaultMinAffinity) = {
    val labels = wRdd.map { case (vid, vect) => vid}.collect
    val edgesRdd = wRdd.flatMap { case (vid, vect) =>
      for ((dval, ix) <- vect.toArray.zipWithIndex
           if Math.abs(dval) >= minAffinity)
      yield Edge(vid, labels(ix), dval)
    }
    edgesRdd
  }

  /**
   * Create the normalized affinity matrix "W" given a set of Points
   *
   * @param sc SparkContext
   * @param points  Input Points in format of [(VertexId,(x,y)]
   *                where VertexId is a Long
   * @param sigma Gaussian parameter sigma
   * @return
   */
  def createNormalizedAffinityMatrix(sc: SparkContext, points: Points, sigma: Double) = {
    val nVertices = points.length
    val affinityRddNotNorm = sc.parallelize({
      val ivect = new Array[IndexedVector[Double]](nVertices)
      for (i <- 0 until points.size) {
        ivect(i) = new IndexedVector(points(i)._1, new BDV(Array.fill(nVertices)(100.0)))
        for (j <- 0 until points.size) {
          val dist = if (i != j) {
            gaussianDist(points(i)._2, points(j)._2, sigma)
          } else {
            0.0
          }
          ivect(i)._2(j) = dist
        }
      }
      ivect.zipWithIndex.map { case (vect, ix) =>
        (ix, vect)
      }
    }, nVertices)
    if (logger.isDebugEnabled) {
      logger.debug(s"Affinity:\n${
        RDDLA.printMatrix(affinityRddNotNorm.map(_._2), nVertices, nVertices)
      }")
    }
    val rowSums = affinityRddNotNorm.map { case (ix, (vid, vect)) =>
      vect.foldLeft(0.0) {
        _ + _
      }
    }
    val materializedRowSums = rowSums.collect
    val similarityRdd = affinityRddNotNorm.map { case (rowx, (vid, vect)) =>
      (vid, vect.map {
        _ / materializedRowSums(rowx)
      })
    }
    if (logger.isDebugEnabled) {
      logger.debug(s"W:\n${RDDLA.printMatrix(similarityRdd, nVertices, nVertices)}")
    }
    (similarityRdd, materializedRowSums)
  }

}
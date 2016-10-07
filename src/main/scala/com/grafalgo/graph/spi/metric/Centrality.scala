package com.grafalgo.graph.spi.metric

import scala.Iterator
import scala.reflect.ClassTag
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.Pregel
import org.apache.spark.graphx.TripletFields
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.VertexRDD
import com.grafalgo.graph.spi.GraphMetrics.EIGENCENTRALITY
import com.grafalgo.graph.spi.GraphMetrics.MetricSlots
import com.grafalgo.graph.spi.GraphMetrics.PAGERANK
import com.grafalgo.graph.spi.GraphVertex
import com.grafalgo.graph.spi.ScalarOperations.DoubleOps
import com.grafalgo.graph.spi.util.scala.LazyLogging
import org.apache.spark.rdd.RDD
import com.grafalgo.graph.spi.GraphMetricsHolder
import com.grafalgo.graph.spi.GraphEdge
import org.apache.spark.storage.StorageLevel

/*
 * Implementation of the eigenvector centrality metric
 */
object Centrality extends LazyLogging {

  /*
   * This metric uses the power iteration algorithm on the graph adjacency matrix to compute the principal eigenvector.
   * 
   * IMPORTANT this implementation follows the Gephi power iteration method.
   * 
   * In particular:
   *  values are normalized using the max dimension instead of the sum of dimensions.
   *  Iterations are always a fixed number and the default value is 100
   *  Starting vector is initialized with 1s
   * 
   */
  //TODO Limit iterations number by using a convergence tolerance parameter
  def computeEigenVectorCentralityMetric[V <: GraphVertex, E <: GraphEdge](holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E], iterations: Int, isDirected: Boolean)(implicit tag: ClassTag[V], etag: ClassTag[E]): RDD[Tuple2[VertexId, GraphMetricsHolder]] = {

    val triplet = new TripletFields(true, true, false)
    def undirectedVector(graph: Graph[(Double, Double), Int]): VertexRDD[(Double, Double)] =
      graph.aggregateMessages(ctx => {
        ctx.sendToSrc((ctx.dstAttr._1, ctx.srcAttr._2)); ctx.sendToDst((ctx.srcAttr._1, ctx.dstAttr._2))
      }, (x, y) => (x._1 + y._1, x._2), triplet)

    def directedVector(graph: Graph[(Double, Double), Int]): VertexRDD[(Double, Double)] =
      graph.aggregateMessages(ctx => {
        ctx.sendToDst((ctx.srcAttr._1, ctx.dstAttr._2))
        ctx.sendToSrc((0, ctx.srcAttr._2))
      }, (x, y) => (x._1 + y._1, x._2), triplet)

    val computeUpdatedVector = if (isDirected) directedVector _ else undirectedVector _

    val init = (1d, 0d)

    var curGraph: Graph[Tuple2[Double, Double], Int] = graph.mapEdges { edge => edge.attr.weight }.mapVertices { (_, eValue) => init }.cache

    for (i <- 1 to iterations) {

      val updatedVector = computeUpdatedVector(curGraph).mapValues(x => x._1 + x._2).cache

      var normFactor = updatedVector.map(_._2).max
      var prevGraph = curGraph
      curGraph = prevGraph.outerJoinVertices(updatedVector) { (_, oldValue, newValue) =>
        val newv = newValue.getOrElse(0d)
        if (normFactor == 0) (0d, newv) else (newv / normFactor, newv)
      }.cache

      curGraph.edges.foreachPartition { x => None }
      prevGraph.unpersist(false)
      prevGraph.edges.unpersist(false)
      updatedVector.unpersist(false)

      logger.info("EigenVector power iteration: " + i)
    }
    val nv = (0.0,0.0)
    val result:RDD[Tuple2[VertexId, GraphMetricsHolder]] = holderRDD.leftOuterJoin(curGraph.vertices)
    .map({ case (id, (h, e)) => (id, h.withMetric(eigenCentrality = e.getOrElse(nv)._1)) })
    result.persist(StorageLevel.MEMORY_AND_DISK)
    result.foreachPartition { x => None }

    curGraph.unpersist(false)
    curGraph.edges.unpersist(false)

    result
  }

  /*
   * Customized version of the pregel pagerank dynamic algorithm including support for undirected graphs and 
   * an iterations limit 
   */
  def computePageRankMetric[V <: GraphVertex, E](holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E], resetProb: Double, tol: Double,
    iterations: Int, isDirected: Boolean)(implicit tag: ClassTag[V], etag: ClassTag[E]): RDD[Tuple2[VertexId, GraphMetricsHolder]] = {

    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 1.0.

    val degreeGraph = if (isDirected) graph.outDegrees else graph.degrees
    val pagerankGraph: Graph[(Double, Double), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(degreeGraph) {
        (vid, vdata, deg) => deg.getOrElse(0)
      }
      // Set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr)
      // Set the vertex attributes to (initalPR, delta = 0)
      .mapVertices((id, attr) => (0.0, 0.0))
      .cache()

    val src: VertexId = -1L

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }

    def personalizedVertexProgram(id: VertexId, attr: (Double, Double),
      msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      var teleport = oldPR
      val delta = if (src == id) 1.0 else 0.0
      teleport = oldPR * delta

      val newPR = teleport + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }

    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
      if (edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
      }
      else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // The initial message received by all vertices in PageRank
    val initialMessage = resetProb / (1.0 - resetProb)

    // Execute a dynamic version of Pregel.
    val vp =
      (id: VertexId, attr: (Double, Double), msgSum: Double) =>
        vertexProgram(id, attr, msgSum)

    val direction = if (isDirected) EdgeDirection.Out else EdgeDirection.Either

    val pregel = Pregel(pagerankGraph, initialMessage, iterations, activeDirection = direction)(
      vp, sendMessage, messageCombiner)

    val result = holderRDD.leftOuterJoin(pregel.mapVertices((vid, attr) => attr._1).vertices).map({ case (id, (h, p)) => (id, h.withMetric(pageRank = p.getOrElse[Double](0.0))) })
    result.persist(StorageLevel.MEMORY_AND_DISK)
    result.foreachPartition { x => None }

    pregel.unpersist(false)
    pregel.edges.unpersist(false)

    result
  }

}
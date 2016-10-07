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
import scala.collection.mutable.LinkedList
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.Accumulator
import com.grafalgo.graph.spi.GraphMetricsHolder
import com.grafalgo.graph.spi.GraphEdge
import java.util.Date
import com.grafalgo.graph.spi.NetworkParameters

/*
 * Implementation of the between centrality metric
 */
object Betweenness extends LazyLogging {

  def computeCentralityMetric[V <: GraphVertex, E <: GraphEdge](sc:SparkContext, holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E], p:NetworkParameters)(implicit tag: ClassTag[V], etag: ClassTag[E]): RDD[Tuple2[VertexId, GraphMetricsHolder]] = {    
    val hbseCore = new HighBetweennessCore(p)
    val (betweennessSet, betweennessGraph) = hbseCore.runHighBetweennessSetExtraction(sc, graph)
    
    val betweennessRDD = betweennessGraph.vertices.map({ case (vid, vertexData) => (vid, vertexData.getApproximateBetweenness) })    
    
    logger.info("betweennessSet count " + betweennessSet.count())
    logger.info("betweennessSet count distinct " + betweennessSet.distinct().count())
    logger.info("betweennessGraph numVertices " + betweennessGraph.numVertices)
    val graphVertex = graph.outerJoinVertices(betweennessRDD)( (vid, v, b) => (vid, v.toString, b.getOrElse(-1.0))).vertices
    println("vid not mapped")
    graphVertex.filter(_._2._3 < 0.0).take(10).foreach(println)
    println("(vid, value) where value == 0: ")
    graphVertex.filter(_._2._3 == 0.0).take(10).foreach(println)
    println("(vid, value) where value > 0: ")
    val supRDD = graphVertex.filter(_._2._3 > 0.0)
    println(supRDD.count())
    supRDD.take(100).foreach(println)    
    holderRDD.join(betweennessRDD).map({ case (id, (h, betweenness)) => (id, h.withMetric(betweennessCentrality = betweenness)) })    
  }


}


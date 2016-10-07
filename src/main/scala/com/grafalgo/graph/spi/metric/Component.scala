package com.grafalgo.graph.spi.metric

import com.grafalgo.graph.spi.util.scala.LazyLogging
import com.grafalgo.graph.spi.GraphEntity
import com.grafalgo.graph.spi.GraphVertex
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.VertexId
import scala.reflect.ClassTag
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.TripletFields
import com.grafalgo.graph.spi.GraphMetrics._
import org.apache.spark.rdd.RDD
import com.grafalgo.graph.spi.GraphMetricsHolder
import com.grafalgo.graph.spi.GraphMetricsHolder
import org.apache.spark.graphx.PartitionStrategy
import com.grafalgo.graph.spi.GraphMetricsHolder
import com.grafalgo.graph.spi.GraphEdge
import org.apache.spark.graphx.Edge
import com.grafalgo.graph.spi.NetworkParameters
import org.apache.spark.storage.StorageLevel

object Component extends LazyLogging {

  def computeMetric[V <: GraphVertex, E <: GraphEdge](holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E])(implicit tag: ClassTag[V], etag: ClassTag[E]): RDD[Tuple2[VertexId, GraphMetricsHolder]] = {

    val ccGraph = graph.connectedComponents
    
    ccGraph.edges.foreachPartition {x => None}

    val result = holderRDD.join(ccGraph.vertices).map({ case (id, (h, c)) => (id, h.withMetric(component = c)) })
    result.persist(StorageLevel.MEMORY_AND_DISK)
    result.foreachPartition { x => None }

    ccGraph.unpersist(false)
    ccGraph.edges.unpersist(false)

    result
  }

  def extractGiantComponent[V <: GraphVertex, E <: GraphEdge](graph: Graph[V, E])(implicit tag: ClassTag[V], etag: ClassTag[E]): Graph[V, E] = {
    logger.info("extractGiantComponent")

    val start = System.nanoTime
    
    val ccGraph = graph.connectedComponents
    
    ccGraph.edges.foreachPartition {x => None}
    
    val biggestClusterId = ccGraph.vertices.map(x => (x._2, 1L)).reduceByKey(_ + _).max()(Ordering[Long].on(_._2))._1
    
    val clusterVertices = ccGraph.vertices.filter(x => x._2 == biggestClusterId)
   
    val result = (graph.outerJoinVertices(clusterVertices) { (_, v, comp) => (v, comp) })
      .subgraph(vpred = (id, comp) => comp._2.isDefined).mapVertices((_, x) => x._1)
      
    result.edges.foreachPartition { x => None }

    logger.info("Time consumed (ms): " + (System.nanoTime() - start) / 1000000)

    ccGraph.unpersist(false)
    ccGraph.edges.unpersist(false)

    result
  }
  
}
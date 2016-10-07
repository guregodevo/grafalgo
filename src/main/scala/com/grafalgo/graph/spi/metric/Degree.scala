package com.grafalgo.graph.spi.metric

import scala.reflect.ClassTag
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.TripletFields
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.VertexId
import com.grafalgo.graph.spi.GraphEntity
import com.grafalgo.graph.spi.GraphEdge
import com.grafalgo.graph.spi.util.scala.LazyLogging
import com.grafalgo.graph.spi.GraphVertex
import com.grafalgo.graph.spi.GraphMetrics._
import org.apache.spark.rdd.RDD
import com.grafalgo.graph.spi.GraphMetricsHolder
import org.apache.spark.storage.StorageLevel

/*
 * Implementation of all the degree metrics
 */

object Degree extends LazyLogging {
  /*
   * Graph degree
   */
  def computeDegreeMetric[V <: GraphVertex, E <: GraphEntity](holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E])(implicit tag: ClassTag[V]): RDD[Tuple2[VertexId, GraphMetricsHolder]] = {
    
    val degrees: VertexRDD[Int] = graph.aggregateMessages(ctx => { ctx.sendToSrc(1); ctx.sendToDst(1) }, _ + _,
      TripletFields.None)
    
    val result = holderRDD.leftOuterJoin(degrees).map({ case (id, (h, d)) => (id, h.withMetric(degree = d.getOrElse[Int](0))) })
    result.persist(StorageLevel.MEMORY_AND_DISK)
    result.foreachPartition { x => None }
    
    result
  }

  /*
   * Graph in degree + out degree +  degree
   */
  def computeDegreesMetric[V <: GraphVertex, E <: GraphEntity](holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E])(implicit tag: ClassTag[V]): RDD[Tuple2[VertexId, GraphMetricsHolder]] = {

    val degrees: VertexRDD[Array[Int]] = graph.aggregateMessages(ctx => {
      ctx.sendToSrc(Array(1, 0));
      ctx.sendToDst(Array(0, 1))
    }, (x, y) => { x(0) += y(0); x(1) += y(1); x },
      TripletFields.None)
    val nv = Array(0,0)
    val result = holderRDD.leftOuterJoin(degrees)
    .map({ case (id, (h, od)) =>{val d = od.getOrElse(nv); (id, h.withMetric(degree = d(1) + d(0), inDegree = d(1), outDegree = d(0)))} })
    result.persist(StorageLevel.MEMORY_AND_DISK)
    result.foreachPartition { x => None }

    result
  }

  /*
   * Graph degree + weighted degree
   */
  def computeWeightedDegreeMetric[V <: GraphVertex, E <: GraphEdge](holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E])(implicit tag: ClassTag[V]): RDD[Tuple2[VertexId, GraphMetricsHolder]] = {

    val degrees: VertexRDD[Array[Int]] = graph.aggregateMessages(ctx => {
      ctx.sendToSrc(Array(1, ctx.attr.weight));
      ctx.sendToDst(Array(1, ctx.attr.weight))
    },
      (x, y) => { x(0) += y(0); x(1) += y(1); x },
      TripletFields.None)
    val nv = Array(0,0)
    val result = holderRDD.leftOuterJoin(degrees)
    .map({ case (id, (h, od)) =>{val d = od.getOrElse(nv); (id, h.withMetric(degree = d(0), weightedDegree = d(1))) }})
    result.persist(StorageLevel.MEMORY_AND_DISK)
    result.foreachPartition { x => None }

    result
  }

  /*
   * Graph degree + in degree + out degree + weighted degree + weighted in degree + weighted out degree
   */
  def computeWeightedDegreesMetric[V <: GraphVertex, E <: GraphEdge](holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E])(implicit tag: ClassTag[V]): RDD[Tuple2[VertexId, GraphMetricsHolder]] = {

    val degrees: VertexRDD[Array[Int]] = graph.aggregateMessages(ctx => {
      ctx.sendToSrc(Array(1, ctx.attr.weight, 0, 0));
      ctx.sendToDst(Array(0, 0, 1, ctx.attr.weight))
    }, (x, y) => { x(0) += y(0); x(1) += y(1); x(2) += y(2); x(3) += y(3); x },
      TripletFields.None)

    val nv = Array(0,0,0,0)
    val result = holderRDD.leftOuterJoin(degrees)
    .map({ case (id, (h, od)) =>{ val d = od.getOrElse(nv);(id, h.withMetric(degree = d(0) + d(2), inDegree = d(2), outDegree = d(0),
        weightedDegree= d(1)+d(3), weightedInDegree = d(3), weightedOutDegree = d(1)  )) }})
    result.persist(StorageLevel.MEMORY_AND_DISK)
    result.foreachPartition { x => None }

    result

  }
}
package com.grafalgo.graph.exporter

import scala.collection.mutable.ArrayBuffer
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import com.grafalgo.graph.spi.GraphEntity
import com.grafalgo.graph.spi.GraphExporter
import com.grafalgo.graph.spi.ScalarMetricProcessor
import com.grafalgo.graph.spi.GraphVertex
import com.grafalgo.graph.spi.GraphEdge
import org.apache.spark.rdd.RDD
import com.grafalgo.graph.spi.MetricProcessor
import scala.reflect.ClassTag
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
 * Json Exporter
 *
 * object implementing the GraphExporter trait for JSON format
 *
 */
trait JsonExporter[V <: GraphVertex, E <: GraphEntity] extends GraphExporter[V, E] {

  override def getGraphAsString(sc: SparkContext, graph: Graph[V, E], processors: Array[MetricProcessor[_, GraphVertex]]): RDD[String] = {
    graph.vertices.map(getVertexAsString(_, processors)).union(graph.edges.map(getEdgeAsString(_)))
  }

  //null safe value
  protected def opt[T](o: T)(implicit m: ClassTag[T]): Option[T] = {
    if (o == null) None else Some(o)
  }

  override def getFormatLabel = "json";

}
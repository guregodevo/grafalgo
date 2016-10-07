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
import com.grafalgo.graph.spi.GraphMetrics
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.commons.lang3.time.FastDateFormat._

/**
 * Gephi Exporter
 *
 * object implementing the GraphExporter trait for Gephi format
 *
 * @author guregodevo
 */
//TODO Overhaul using Scala XML
trait GephiExporter[V <: GraphVertex, E <: GraphEntity] extends GraphExporter[V, E] {
  //Gefx xml format constants

  final val XML_OPENING_TAG = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  final val GEXF_OPENING_TAG = "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n"
  final val GRAPH_OPENING_TAG_DIRECTED = "<graph mode=\"static\" defaultedgetype=\"directed\">\n"
  final val GRAPH_OPENING_TAG_UNDIRECTED = "<graph mode=\"static\" defaultedgetype=\"undirected\">\n"
  final val NODES_OPENING_TAG = " <nodes>\n"
  final val NODE_OPENING_TAG = "  <node id=\""
  final val LABEL_PARAM = "\" label=\""
  final val ATTRIBUTES_OPENING_TAG = "\t<attvalues>\n"
  final val ATTRIBUTE_OPENING_TAG = "\t\t<attvalue for=\""
  final val ATTRIBUTE_VALUE_PARAM = "\" value=\""
  final val CLOSING_TAG = "\"/>\n"
  final val END_TAG = "\">\n"
  final val ATTRIBUTES_CLOSING_TAG = "\t</attvalues>"
  final val NODE_CLOSING_TAG = "\n  </node>"
  final val EDGES_OPENING_TAG = " <edges>\n"
  final val EDGE_OPENING_TAG = "  <edge source=\""
  final val EDGE_CLOSING_TAG = "\n  </edge>"
  final val EDGE_TARGET_PARAM = "\" target=\""
  final val EDGE_WEIGHT_PARAM = "\" weight=\""
  final val NODES_CLOSING_TAG = " </nodes>\n"
  final val EDGES_CLOSING_TAG = " </edges>\n"
  final val GRAPH_CLOSING_TAG = "</graph>\n"
  final val GEXF_CLOSING_TAG = "</gexf>"

  final val NODES_ATTRIBUTES_TAG = "<attributes class=\"node\" mode=\"static\">"
  final val EDGES_ATTRIBUTES_TAG = "<attributes class=\"edge\" mode=\"static\">"
  final val CLOSING_ATTRIBUTES_TAG = "</attributes>"

  final val GEXF_HEADER_DIRECTED = XML_OPENING_TAG + GEXF_OPENING_TAG + GRAPH_OPENING_TAG_DIRECTED + NODES_OPENING_TAG
  final val GEXF_HEADER_UNDIRECTED = XML_OPENING_TAG + GEXF_OPENING_TAG + GRAPH_OPENING_TAG_UNDIRECTED + NODES_OPENING_TAG
  final val GEXF_FOOTER = EDGES_CLOSING_TAG + GRAPH_CLOSING_TAG + GEXF_CLOSING_TAG
  final val GEXF_SEPARATOR = NODES_CLOSING_TAG + EDGES_OPENING_TAG

  final val ITEM_SEPARATOR = "||"

  final val GEPHI_DATE_FORMAT_DEFINITION = "dd/MM/yyyy hh:mm";
  final val GEPHI_DATE_FORMAT = FastDateFormat.getInstance(GEPHI_DATE_FORMAT_DEFINITION)

  //Char processing

  val SPECIAL_CHARS = Array("&", "\"", "<", ">")
  val REPLACEMENTS = Array("&amp;", "&quot;", "&lt;", "&gt;")
  
  def formatGephiDate(date:Long) = {
    GEPHI_DATE_FORMAT.format(new Date(date))
  }

  override def getFormatLabel = "gexf";

  override def getGraphAsString(sc: SparkContext, graph: Graph[V, E], processors: Array[MetricProcessor[_, GraphVertex]]) = {
    sc.parallelize(getHeader(getMetricHeader(processors)).split("\n").toList) ++
      getVerticesAsString(sc, graph, processors) ++
      sc.parallelize(GEXF_SEPARATOR.split("\n").toList) ++
      getEdgesAsString(sc, graph, processors) ++
      sc.parallelize(GEXF_FOOTER.split("\n").toList)
  }

  def getVerticesAsString(sc: SparkContext, graph: Graph[V, E], processors: Array[MetricProcessor[_, GraphVertex]]): RDD[String] = {
    graph.vertices.map(getVertexAsString(_, processors))
  }

  def getEdgesAsString(sc: SparkContext, graph: Graph[V, E], processors: Array[MetricProcessor[_, GraphVertex]]): RDD[String] = {
    graph.edges.map(getEdgeAsString)
  }

  override def getVertexAsString(vertex: Tuple2[VertexId, V], processors: Array[MetricProcessor[_, GraphVertex]]): String = {
    val builder = new StringBuilder(NODE_OPENING_TAG) ++= vertex._1.toString ++= LABEL_PARAM ++=
      processSpecialChars(getVertexLabel(vertex._2)) ++= END_TAG ++= ATTRIBUTES_OPENING_TAG
    getVertexProperties(vertex._2).map(pair => builder ++= ATTRIBUTE_OPENING_TAG ++= pair._1 ++=
      ATTRIBUTE_VALUE_PARAM ++= processSpecialChars(pair._2) ++= CLOSING_TAG)
    for (p <- processors) { builder ++= ATTRIBUTE_OPENING_TAG ++= p.name ++= ATTRIBUTE_VALUE_PARAM ++= p.get(vertex._2).toString ++= CLOSING_TAG }
    builder ++= ATTRIBUTES_CLOSING_TAG ++= NODE_CLOSING_TAG

    builder.toString
  }

  protected def getVertexProperties(vertex: V): ArrayBuffer[(String, String)]

  protected def getVertexLabel(vertex: V): String

  override def getEdgeAsString(edge: Edge[E]): String = {
    val weight = edge.attr match {
      case e: GraphEdge => e.weight
      case _ => 1
    }
    val builder = new StringBuilder(EDGE_OPENING_TAG) ++= edge.srcId.toString ++= EDGE_TARGET_PARAM ++= edge.dstId.toString ++=
      LABEL_PARAM ++= processSpecialChars(getEdgeLabel(edge.attr))
    if (weight > 1) { builder ++= EDGE_WEIGHT_PARAM ++= weight.toString }
    builder ++= END_TAG ++= ATTRIBUTES_OPENING_TAG
    getEdgeProperties(edge.attr).map(pair => builder ++= ATTRIBUTE_OPENING_TAG ++= pair._1 ++= ATTRIBUTE_VALUE_PARAM ++=
      processSpecialChars(pair._2) ++= CLOSING_TAG)
    builder ++= ATTRIBUTES_CLOSING_TAG ++= EDGE_CLOSING_TAG

    builder.toString
  }

  protected def getHeader(metricHeader: String) = GEXF_HEADER_DIRECTED

  protected def getMetricHeader(processors: Array[MetricProcessor[_, GraphVertex]]): String =
    if (processors.size != 0)
      (for (p <- processors) yield ("<attribute id=\"" + p.name + "\" title=\"" + p.name + "\" type=\"" + metricTypes(p.name) + "\"/>\n")).mkString("")
    else ""

  protected def getEdgeProperties(edge: E): ArrayBuffer[(String, String)]

  protected def getEdgeLabel(edge: E): String

  def processSpecialChars(stringRepr: String) =
    if (stringRepr != null)
      StringUtils.replaceEach(stringRepr, SPECIAL_CHARS, REPLACEMENTS)
    else stringRepr

  val metricTypes = Map(GraphMetrics.EIGENCENTRALITY.toString -> "double",
    GraphMetrics.PAGERANK.toString -> "double",
    GraphMetrics.BETWEENNESSCENTRALITY.toString -> "double",
    GraphMetrics.COMPONENT.toString -> "integer",
    GraphMetrics.MODULARITY.toString -> "integer",
    GraphMetrics.DEGREE.toString -> "integer",
    GraphMetrics.INDEGREE.toString -> "integer",
    GraphMetrics.OUTDEGREE.toString -> "integer",
    GraphMetrics.WEIGHTEDDEGREE.toString -> "integer",
    GraphMetrics.WEIGHTEDINDEGREE.toString -> "integer",
    GraphMetrics.WEIGHTEDOUTDEGREE.toString -> "integer")

}
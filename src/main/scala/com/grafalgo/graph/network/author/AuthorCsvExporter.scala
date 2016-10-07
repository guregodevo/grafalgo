package com.grafalgo.graph.network.author

import com.grafalgo.graph.network.author._
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Edge
import com.grafalgo.graph.exporter.CsvExporter
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
import com.grafalgo.graph.exporter.CsvExporter
import com.grafalgo.graph.spi.ServiceFactory
import com.grafalgo.graph.spi.NetworkParameters
import com.grafalgo.graph.source.GephiDataSource._


object AuthorCsvExporter extends CsvExporter[AuthorVertex, AuthorEdge] {

  protected final val ITEM_SEPARATOR = "||"

  
  override def getVertexHeader(): Array[String] = {
    Array("Id", "Label", "NAME", "PERSONAL-WEBSITE", "ALL-NICK-ACTIVITY-EVER", "NICK-FOLLOWERS", "COUNTRY", "LOCATION", "Urls Sent", "Domains Sent", "Type", "FIRST PUBDATE", "LAST PUBDATE", "Closeness Centrality", "Betweenness Centrality")
  }
  
  def getVertexAsString(v: Tuple2[Long, AuthorVertex], processors: Array[MetricProcessor[_, GraphVertex]]): String = {
    ArrayBuffer(
        v._1.toString,
        v._2.nickName,
        v._2.nickName, 
        opt(v._2.webSite),
      opt(v._2.activity),
      opt(v._2.rating),
      opt(v._2.country),
      opt(v._2.location),
      opt(v._2.urlNumber),
      opt(v._2.domainNumber),
      "type",
      opt(v._2.firstItemDate),
      opt(v._2.lastItemDate))
      .mkString(SEPARATOR)
  }

  def getEdgeAsString(e: org.apache.spark.graphx.Edge[AuthorEdge], id: Long): String = {
    val weight = e.attr match {
      case e: GraphEdge => e.weight
      case _ => 1
    }
    ArrayBuffer(e.srcId.toString,
      e.dstId.toString,
      if (weight != 1) { DIRECTED } else { UNDIRECTED },
      id.toString,
      if (weight > 1) { e.attr.firstItemBody + ITEM_SEPARATOR + e.attr.lastItemLink } else e.attr.firstItemBody,
      opt(weight),
      if (weight > 1) { e.attr.firstItemLink + ITEM_SEPARATOR + e.attr.lastItemLink } else e.attr.firstItemLink,
      e.attr.firstItemDate.toString,
      e.attr.lastItemDate.toString)
      .mkString(SEPARATOR)
  }

  override def getEdgeHeader(): Array[String] = {
    Array("Source", "Target", "Type", "Id", "Label", "Weight", "LINKS", "FIRST PUBDATE", "LAST PUBDATE")
  }

}

class AuthorCsvExporterFactory extends ServiceFactory[CsvExporter[AuthorVertex, AuthorEdge]] {
  override def service(params: NetworkParameters) = AuthorCsvExporter
}



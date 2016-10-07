package com.grafalgo.graph.network.ego

import scala.xml.XML
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import com.grafalgo.graph.source.GephiDataSource
import com.grafalgo.graph.source.GephiDataSource.EDGE_TAG_PREFIX
import com.grafalgo.graph.source.GephiDataSource.ID_ATTR
import com.grafalgo.graph.source.GephiDataSource.LABEL_ATTR
import com.grafalgo.graph.source.GephiDataSource.NODE_TAG_PREFIX
import com.grafalgo.graph.source.GephiDataSource.SOURCE_ATTR
import com.grafalgo.graph.source.GephiDataSource.TARGET_ATTR
import com.grafalgo.graph.source.GephiDataSource.WEIGHT_ATTR
import com.grafalgo.graph.spi.ServiceFactory
import com.grafalgo.graph.spi.NetworkParameters
import com.grafalgo.graph.spi.GraphMetrics._
import com.grafalgo.graph.spi.ScalarMetricProcessor
import com.grafalgo.graph.spi.GraphMetrics
import com.grafalgo.graph.spi.MetricProcessor
import com.grafalgo.graph.spi.GraphVertex

/**
 * DataSource for loading a gephi file
 *
 * Using the scala native xml api to perform the xml parsing. There are some concerns about memory usage (DOM model) but
 * we are scanning a small block at a time.
 *
 * The ideal solution would be a jackson mixin but we don't want to manage the dependencies involved and the xml support
 * needs are limited to these simple gephi files so far.
 *
 * @author guregodevo
 */
object EgoGephiDataSource extends GephiDataSource[ItemEgoData, AuthorVertex, AuthorEdge] {

  override def parseNode(xmlBlock: String, processors: Array[MetricProcessor[_, GraphVertex]]): (Long, AuthorVertex) = {
    val nodeElem = XML.loadString(xmlBlock)
    val id = (nodeElem \ ID_ATTR).text.toLong
    val attrs = getAttributes(nodeElem)

    val node = new AuthorVertex(attrs.getOrElse(NAME, null), attrs(AUTHORID).toLong, attrs(ACTIVITY).toInt, attrs(RATING).toInt, attrs.getOrElse(COUNTRY, null), attrs.getOrElse(LOCATION, null),
      attrs.getOrElse(WEBSITE, null), parseGephiDate(attrs(FIRSTDATE)), parseGephiDate(attrs(LASTDATE)), attrs(URLS).toInt, attrs(DOMAINS).toInt, null, null)

    processors.foreach(processor => processor.putString(attrs(processor.name), node))

    (id, node)
  }

  override def parseEdge(xmlElem: String): Edge[AuthorEdge] = {

    val edgeElem = XML.loadString(xmlElem.toString)

    val source = (edgeElem \ SOURCE_ATTR).text.toLong
    val target = (edgeElem \ TARGET_ATTR).text.toLong
    val weight = (edgeElem \ WEIGHT_ATTR).text match { case "" => 1; case x => x.toInt }

    val (firstBody, lastBody) = getPairValues((edgeElem \ LABEL_ATTR).text.toString)

    val attrs = getAttributes(edgeElem)

    val (firstLink, lastLink) = getPairValues(attrs.getOrElse(LINKS, null))

    new Edge(source, target, new AuthorEdge(weight, firstBody, lastBody, firstLink, lastLink,
        parseGephiDate(attrs(FIRSTDATE)), parseGephiDate(attrs(LASTDATE))))
  }

}

class EgoGephiDataSourceFactory extends ServiceFactory[GephiDataSource[ItemEgoData, AuthorVertex, AuthorEdge]] {
  override def service(params: NetworkParameters) = EgoGephiDataSource
}

package com.grafalgo.graph.network.ego

import scala.collection.mutable.ArrayBuffer

import com.grafalgo.graph.exporter._
import com.grafalgo.graph.exporter.GephiExporter
import com.grafalgo.graph.spi.GraphEdge
import com.grafalgo.graph.spi.NetworkParameters
import com.grafalgo.graph.spi.ServiceFactory

object EgoGephiExporter extends GephiExporter[AuthorVertex, AuthorEdge] {

  protected def getEdgeLabel(edge: AuthorEdge): String = if (edge.weight > 1) edge.firstItemBody + ITEM_SEPARATOR + edge.lastItemBody else edge.firstItemBody
  
  protected def getEdgeProperties(e: AuthorEdge): ArrayBuffer[(String, String)] = {
    ArrayBuffer((LINKS, if (e.weight > 1) (e.firstItemLink + ITEM_SEPARATOR + e.lastItemLink) else e.firstItemLink),
      (FIRSTDATE, formatGephiDate(e.firstItemDate)), (LASTDATE, formatGephiDate(e.lastItemDate)))
      .filter(_._2 != null)
  }
  
  protected def getVertexLabel(vertex: AuthorVertex): String = vertex.nickName
  
  protected def getVertexProperties(v: AuthorVertex): ArrayBuffer[(String, String)] = {
    ArrayBuffer((NAME, v.nickName), (AUTHORID, v.nickId + ""), (WEBSITE, v.webSite), (ACTIVITY, "" + v.activity),
      (RATING, "" + v.rating), (COUNTRY, v.country), (LOCATION, v.location),
      (URLS, "" + v.urlNumber), (DOMAINS, "" + v.domainNumber), (TYPE, TYPE_VAL),
      (FIRSTDATE, formatGephiDate(v.firstItemDate)), (LASTDATE, formatGephiDate(v.lastItemDate)))
      .filter(_._2 != null)
  }
  
  override def getHeader(metricHeader: String) = GEXF_HEADER_DIRECTED + "\n" + NODES_ATTRIBUTES_TAG + "\n" + NAME_ATTR + "\n" + ACTIVITY_ATTR + "\n" + FOLLOWERS_ATTR + "\n" +
    COUNTRY_ATTR + "\n"  + LOCATION_ATTR +"\n" + URLS_ATTR + "\n" + DOMAINS_ATTR + "\n" + TYPE_ATTR + "\n" + FIRST_DATE_ATTR + "\n" + LAST_DATE_ATTR + "\n" + metricHeader + CLOSING_ATTRIBUTES_TAG + "\n" +
    EDGES_ATTRIBUTES_TAG + "\n" + LINKS_ATTR + "\n" + FIRST_DATE_ATTR + "\n" + LAST_DATE_ATTR + "\n" + CLOSING_ATTRIBUTES_TAG + "\n"

}

class EgoGephiExporterFactory extends ServiceFactory[GephiExporter[AuthorVertex, AuthorEdge]] {
  override def service(params: NetworkParameters) = EgoGephiExporter
}
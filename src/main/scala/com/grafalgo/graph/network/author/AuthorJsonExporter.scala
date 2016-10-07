package com.grafalgo.graph.network.author

import org.apache.spark.graphx.Edge
import org.json4s.JsonDSL.int2jvalue
import org.json4s.JsonDSL.jobject2assoc
import org.json4s.JsonDSL.long2jvalue
import org.json4s.JsonDSL.pair2Assoc
import org.json4s.JsonDSL.pair2jvalue
import org.json4s.JsonDSL.seq2jvalue
import org.json4s.JsonDSL.string2jvalue
import org.json4s.jackson.JsonMethods.compact
import org.json4s.jackson.JsonMethods.render
import com.grafalgo.graph.exporter.JsonExporter
import com.grafalgo.graph.spi.GraphExporter
import com.grafalgo.graph.spi.GraphVertex
import com.grafalgo.graph.spi.MetricProcessor
import com.grafalgo.graph.spi.NetworkParameters
import com.grafalgo.graph.spi.ServiceFactory
import java.util.Collections.EmptySet
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object AuthorJsonExporter extends JsonExporter[AuthorVertex, AuthorEdge] {  
  
  
  val AUTHOR_TYPE_JSON = ("_type" -> "author")
  val EDGE_TYPE_JSON = ("_type" -> "edge")
  val AUTHOR_TYPE_STRING = "{\"_type\":\"" +  "author" 
  val EDGE_TYPE_STRING = "{\"_type\":\"" + "edge"
  
  override def getVertexAsString(vertex: Tuple2[Long, AuthorVertex], processors: Array[MetricProcessor[_, GraphVertex]]): String = {   
    val obj = vertex._2
    var json = AUTHOR_TYPE_JSON ~
            ("id" -> vertex._1) ~
            ("name" -> opt(obj.nickName)) ~
            ("activity" -> opt(obj.activity)) ~
            ("rating" -> opt(obj.rating)) ~
            ("country" -> opt(obj.country)) ~
            ("location" -> opt(obj.location)) ~
            ("urlNumber" -> opt(obj.urlNumber)) ~
            ("domainNumber" -> opt(obj.domainNumber)) ~
            ("firstItemDate" -> opt(obj.firstItemDate)) ~
            ("lastItemDate" -> opt(obj.lastItemDate)) ~                
            ("webSite" -> opt(obj.webSite))
 
    processors.foreach { p => json ~= (p.name.toLowerCase() -> p.get(vertex._2).toString()) }
    
    compact(render( { json } ))
  }

  
  override def getEdgeAsString(edge: Edge[AuthorEdge]): String = {
    val json = 
        EDGE_TYPE_JSON ~
        ("src" -> edge.srcId) ~
        ("dst" -> edge.dstId) ~
        ("weight" -> edge.attr.weight) ~
        ("firstItemLink" -> opt(edge.attr.firstItemLink))  ~
        ("lastItemLink" -> opt(edge.attr.lastItemLink))  ~
        ("firstItemDate" -> opt(edge.attr.firstItemDate))  ~
        ("lastItemDate" -> opt(edge.attr.lastItemDate))
    
    compact(render(json))
  }

}

class AuthorJsonExporterFactory extends ServiceFactory[GraphExporter[AuthorVertex, AuthorEdge]] {
  override def service(params: NetworkParameters) = AuthorJsonExporter
}

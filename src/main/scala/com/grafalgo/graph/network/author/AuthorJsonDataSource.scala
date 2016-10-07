package com.grafalgo.graph.network.author

import com.grafalgo.graph.source.JsonDataSource
import com.grafalgo.graph.spi.ServiceFactory
import com.grafalgo.graph.spi.GraphVertex
import com.grafalgo.graph.spi.NetworkParameters
import com.grafalgo.graph.spi.ScalarMetricProcessor
import org.apache.spark.graphx.Edge
import org.json4s.FieldSerializer
import org.json4s.FieldSerializer._
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object AuthorJsonDataSource extends JsonDataSource[ItemAuthorData, AuthorVertex, AuthorEdge] {

  import AuthorJsonExporter._

  override def parseVertex(json: String): (Long, AuthorVertex) = {
    implicit val format = DefaultFormats
 
    if (json.startsWith(AUTHOR_TYPE_STRING)) {
      val o = parse(json)        
      val v = for {        
          JObject(_) <- o.toOption
          JInt(id) <- (o \ "id").toOption
          JString(nickName) <- (o \ "name").toOption
          JInt(activity) <- (o \ "activity").toOption
          JInt(rating) <- (o \ "rating").toOption
          //JString(country) <- (o \ "country").toOption
          //JString(location) <- (o \ "location").toOption
          JInt(firstItemDate) <- (o \ "firstItemDate").toOption
          JInt(lastItemDate) <- (o \ "lastItemDate").toOption
          JInt(urlNumber) <- (o \ "urlNumber").toOption
          JInt(domainNumber) <- (o \ "domainNumber").toOption
          //JString(webSite) <- (o \ "webSite").toOption
          } yield (id.longValue(), new AuthorVertex(nickName,activity.longValue(),rating.longValue(),null,null,null,firstItemDate.longValue(),lastItemDate.longValue(),urlNumber.toInt,domainNumber.toInt, false))
      v.getOrElse(null.asInstanceOf[(Long, AuthorVertex)])
    } else {
      null
    }
  }

  def parseEdge(json: String): Edge[AuthorEdge] = {
    implicit val formats = DefaultFormats
    if (json.startsWith(EDGE_TYPE_STRING)) {
      val o = parse(json)
      //println(o)
      val v = for {
          JObject(_) <- o.toOption 
          JInt(scrcId) <- (o \ "src").toOption
          JInt(dstId) <- (o \ "dst").toOption
          JInt(weight) <- (o \ "weight").toOption
          /*JString(firstItemBody) <- (o \ "firstItemBody").toOption
          JString(lastItemBody) <- (o \ "lastItemBody").toOption*/
          JString(firstItemLink) <- (o \ "firstItemLink").toOption
          JString(lastItemLink) <- (o \ "lastItemLink").toOption       
          JInt(firstItemDate) <- (o \ "firstItemDate").toOption          
          JInt(lastItemDate) <- (o \ "lastItemDate").toOption          
          } yield new Edge(scrcId.longValue(), dstId.longValue(), new AuthorEdge(weight.intValue(),null,null,firstItemLink, lastItemLink, firstItemDate.longValue(),lastItemDate.longValue()))          
      v.getOrElse(null)
    } else {
      null
    }
  }

  

}

class AuthorJsonDataSourceFactory extends ServiceFactory[JsonDataSource[ItemAuthorData, AuthorVertex, AuthorEdge]] {
  override def service(params: NetworkParameters) = AuthorJsonDataSource
}

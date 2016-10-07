package com.grafalgo.graph.network.ego

import com.grafalgo.graph.spi.GraphEdge
import com.grafalgo.graph.spi.NetworkJob
import com.grafalgo.graph.spi.NetworkParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import com.grafalgo.graph.spi.ServiceFactory
import com.grafalgo.graph.spi.util.scala.LazyLogging
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.graphx.Edge
import com.grafalgo.graph.spi.GraphMetrics._

/**
 * Spark job for creating a ego author network.
 *
 * @author guregodevo
 */
class EgoNetwork(override val parameters: NetworkParameters) extends NetworkJob[ItemEgoData, AuthorVertex, AuthorEdge] {

  import EgoNetwork._

  override def buildGraph(inputRDD: RDD[(Long, ItemEgoData)]): Graph[AuthorVertex, AuthorEdge] = {

    inputRDD.cache

    //Emit vertices
    val vertexRDD = getAuthors(inputRDD)

    //Emit edges
    val edgeRDD = inputRDD.flatMap(emitEdges).cache

    return Graph(removeUnconnectedNodes(vertexRDD, edgeRDD), edgeRDD).distributeGraph(parameters.partitionStrategy, mergeEdges)
  }

  override def isDirected: Boolean = true
  override def isWeighted: Boolean = true
  override val jobName: String = EgoNetwork.jobName
  override val serializableClasses: Array[String] = Array("com.grafalgo.graph.network.ego.ItemEgoData", "com.grafalgo.graph.network.ego.EgoElasticDataSource$$anonfun$3",
    "[Lcom.grafalgo.graph.network.ego.MentionData;", "com.grafalgo.graph.network.ego.MentionData", "com.grafalgo.graph.network.ego.AuthorEdge",
    "[Lcom.grafalgo.graph.network.ego.AuthorEdge;", "com.grafalgo.graph.network.ego.AuthorVertex", "scala.Enumeration$Val", "[Lcom.grafalgo.graph.network.ego.AuthorVertex;")
}

object EgoNetwork {

  val jobName: String = "EgoNetwork"

  def getAuthors(inputRDD: RDD[(Long, ItemEgoData)]) = inputRDD.flatMap(emitAuthors).reduceByKey(mergeAuthors).mapValues(getAuthor)

  private def emitAuthors(item: (Long, ItemEgoData)): ArrayBuffer[(Long, (AuthorVertex))] = {

    //Author
    val result: ArrayBuffer[(Long, (AuthorVertex))] = ArrayBuffer((item._2.nickId, (new AuthorVertex(item._2.nickName, item._2.nickId, item._2.nickActivity, item._2.nickRating,
      item._2.personCountry, item._2.personLocation, item._2.personWebsite, item._2.itemPublishDate,
      item._2.itemPublishDate, 0, 0,
      if (item._2.domains != null) mutable.Set(item._2.domains.filter { _ != null }:_*) else mutable.Set.empty,
      if (item._2.urls != null) mutable.Set(item._2.urls:_*) else mutable.Set.empty))))

    //Mention
    if (item._2.mentions != null) result ++= (for (mention <- item._2.mentions) yield ((mention.authorId, new AuthorVertex(mention.authorName, mention.authorId, mention.authorActivity, mention.authorRating,
      null, null, null, Long.MaxValue, Long.MinValue, 0, 0, mutable.Set.empty, mutable.Set.empty, parentData = true))))

    //Reply
    if (item._2.replies != null) result ++= (for (reply <- item._2.replies) yield ((reply.authorId, new AuthorVertex(reply.authorName, reply.authorId, reply.authorActivity, reply.authorRating,
      null, null, null, Long.MaxValue, Long.MinValue, 0, 0, mutable.Set.empty, mutable.Set.empty, parentData = true))))

    //Parent
    if (item._2.itemParentId > 0) result +=
      ((item._2.parentNickId, (new AuthorVertex(item._2.parentNickName, item._2.parentNickId, item._2.parentNickActivity, item._2.parentNickRating,
        null, null, null, item._2.parentPublishDate, item._2.parentPublishDate, 0, 0, mutable.Set.empty, mutable.Set.empty, parentData = true))))

    result
  }

  private def emitEdges(item: (Long, ItemEgoData)) = {

    val result = ArrayBuffer[Edge[AuthorEdge]]()

    //Parent Edge
    if (item._2.itemParentId > 0) result += new Edge(item._2.nickId, item._2.parentNickId,
      new AuthorEdge(1, item._2.itemBody, item._2.itemBody, item._2.itemLink, item._2.itemLink,
        item._2.itemPublishDate, item._2.itemPublishDate))

    //Mentions Edges
    if (item._2.mentions != null)
      result ++= (for (mention <- item._2.mentions) yield (new Edge(item._2.nickId, mention.authorId,
        new AuthorEdge(1, item._2.itemBody, item._2.itemBody, item._2.itemLink, item._2.itemLink,
          item._2.itemPublishDate, item._2.itemPublishDate))))

    //Replies Edges
    if (item._2.replies != null)
      result ++= (for (reply <- item._2.replies) yield (new Edge(item._2.nickId, reply.authorId,
        new AuthorEdge(1, item._2.itemBody, item._2.itemBody, item._2.itemLink, item._2.itemLink,
          item._2.itemPublishDate, item._2.itemPublishDate))))

    result
  }

  private def mergeAuthors(author1: AuthorVertex, author2: AuthorVertex) = {
    var target = author1
    var contributor = author2
    if (author1.parentData) { target = author2; contributor = author1 }

    target.copy(firstItemDate = Math.min(target.firstItemDate, contributor.firstItemDate),
      lastItemDate = Math.max(target.lastItemDate, contributor.lastItemDate),
      domainSet = if (!contributor.parentData) target.domainSet ++ contributor.domainSet else target.domainSet,
      urlSet = if (!contributor.parentData) target.urlSet ++ contributor.urlSet else target.urlSet)
  }

  private def getAuthor(author: AuthorVertex) =
    author.copy(urlNumber = if (author.urlSet != null) author.urlSet.size else 0,
      domainNumber = if (author.domainSet != null) author.domainSet.size else 0,
      domainSet = null,
      urlSet = null)

  private def mergeEdges(src: AuthorEdge, dst: AuthorEdge): AuthorEdge = {
    var fDate = src.firstItemDate
    var fBody = src.firstItemBody
    var fLink = src.firstItemLink
    var lDate = src.lastItemDate
    var lBody = src.lastItemBody
    var lLink = src.lastItemLink
    if (src.firstItemDate > dst.firstItemDate) {
      fDate = dst.firstItemDate
      fBody = dst.firstItemBody
      fLink = dst.firstItemLink
    }
    if (src.lastItemDate < dst.lastItemDate) {
      lDate = dst.lastItemDate
      lBody = dst.lastItemBody
      lLink = dst.lastItemLink
    }
    new AuthorEdge(src.weight + dst.weight, fBody, lBody, fLink, lLink, fDate, lDate)
  }

}

class EgoNetworkFactory extends ServiceFactory[NetworkJob[ItemEgoData, AuthorVertex, AuthorEdge]] with LazyLogging {
  override def service(params: NetworkParameters) = new EgoNetwork(params)
}
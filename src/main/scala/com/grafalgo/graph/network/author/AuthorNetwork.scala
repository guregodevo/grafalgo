package com.grafalgo.graph.network.author

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.grafalgo.graph.spi.NetworkJob
import com.grafalgo.graph.spi.ServiceFactory
import com.grafalgo.graph.spi.GraphMetrics._
import com.grafalgo.graph.spi.GraphMetrics
import com.grafalgo.graph.spi.NetworkParameters
import com.grafalgo.graph.spi._
import com.typesafe.config.Config
import com.grafalgo.graph.spi.util.scala.LazyLogging
import GraphMetrics._

/**
 * Spark job for creating a authors network
 *
 * @author guregodevo
 */

class AuthorNetwork(override val parameters: NetworkParameters) extends NetworkJob[ItemAuthorData, AuthorVertex, AuthorEdge] {

  import AuthorNetwork._

  override def buildGraph(inputRDD: RDD[(Long, ItemAuthorData)]):Graph[AuthorVertex,AuthorEdge] = {
    inputRDD.cache

    val vertexRDD = inputRDD.flatMap(emitAuthors).reduceByKey(aggregateAuthors).mapValues(getAggregatedAuthor)
    val edgeRDD = inputRDD.filter(_._2.itemParentId > 0)
      .map(item => new Edge(item._2.nickId, item._2.parentNickId,
        new AuthorEdge(1, item._2.itemBody, item._2.itemBody, item._2.itemLink, item._2.itemLink,
          item._2.itemPublishDate, item._2.itemPublishDate)))
          

    Graph(removeUnconnectedNodes(vertexRDD, edgeRDD), edgeRDD).distributeGraph(parameters.partitionStrategy, mergeEdges)
  }

  override val serializableClasses = AuthorNetwork.serializableClasses
  override val jobName = AuthorNetwork.jobName
  override val isDirected = true
  override val isWeighted = true

}

object AuthorNetwork {

  val jobName = "AuthorNetwork"

  private val serializableClasses = Array("com.grafalgo.graph.network.author.AuthorEdge", "com.grafalgo.graph.network.author.ItemAuthorData",
    "com.grafalgo.graph.network.author.AuthorVertex", "com.grafalgo.graph.network.author.AuthorNetwork$AuthorDataAggregation",
    "[Lcom.grafalgo.graph.network.author.AuthorVertex;", "[Lcom.grafalgo.graph.network.author.AuthorEdge;")

  private def emitAuthors(item: (Long, ItemAuthorData)): List[(Long, (AuthorVertex, AuthorDataAggregation))] = {
    val nick = (item._2.nickId, (new AuthorVertex(item._2.nickName, item._2.nickActivity, item._2.nickRating,
      item._2.personCountry, item._2.personLocation, item._2.personWebsite, item._2.itemPublishDate,
      item._2.itemPublishDate, 0, 0),
      new AuthorDataAggregation(if (item._2.domains != null) Set.empty ++ item._2.domains.to else Set.empty,
        if (item._2.urls != null) Set.empty ++ item._2.urls else Set.empty)))
    if (item._2.itemParentId > 0) {
      List(nick,
        (item._2.parentNickId, (new AuthorVertex(item._2.parentNickName, item._2.parentNickActivity, item._2.parentNickRating,
          null, null, null, item._2.parentPublishDate, item._2.parentPublishDate, 0, 0, parentData = true),
          new AuthorDataAggregation(Set.empty, Set.empty))))
    }
    else List(nick)
  }

  private def aggregateAuthors(author1: (AuthorVertex, AuthorDataAggregation), author2: (AuthorVertex, AuthorDataAggregation)) = {

    var target = author1
    var contributor = author2
    if (author1._1.parentData) { target = author2; contributor = author1 }

    val firstItemDate = Math.min(target._1.firstItemDate, contributor._1.firstItemDate)
    val lastItemDate = Math.max(target._1.lastItemDate, contributor._1.lastItemDate)
    val domainSet =  if (!contributor._1.parentData) target._2.domainSet ++ contributor._2.domainSet else target._2.domainSet
    val urlSet = if (!contributor._1.parentData) target._2.urlSet ++ contributor._2.urlSet else target._2.urlSet


    val t = target._1

    val authorData = new AuthorVertex(t.nickName, t.activity, t.rating,
      t.country, t.location, t.webSite, firstItemDate, lastItemDate, t.urlNumber, t.domainNumber, t.parentData)

    val aggData = new AuthorDataAggregation(domainSet, urlSet)

    (authorData, aggData)
  }

  private def getAggregatedAuthor(author: (AuthorVertex, AuthorDataAggregation)) = {

    author._1.copy(urlNumber = Option(author._2.urlSet).map(_.size).getOrElse(0),
        domainNumber  =Option(author._2.domainSet).map(_.size).getOrElse(0))
  }

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

  private class AuthorDataAggregation(val domainSet: Set[String], val urlSet: Set[Long])

}

class AuthorNetworkFactory extends ServiceFactory[NetworkJob[ItemAuthorData, AuthorVertex, AuthorEdge]] with LazyLogging {
  override def service(params: NetworkParameters) = new AuthorNetwork(params)
}
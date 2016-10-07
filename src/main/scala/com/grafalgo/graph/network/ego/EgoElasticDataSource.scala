package com.grafalgo.graph.network.ego

import scala.collection.convert.Wrappers.JListWrapper
import scala.collection.mutable
import org.apache.spark.SparkContext
import org.elasticsearch.spark.sparkContextFunctions
import com.grafalgo.graph.spi._
import com.grafalgo.graph.spi.GraphDataSource
import com.grafalgo.graph.source.ElasticDataSource
import com.grafalgo.graph.source.ElasticDataSource._
import org.apache.spark.rdd.RDD
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._

//TODO This has to be reviewed with the definitive elasticsearch mapping. 
/**
 * DataSource corresponding to an elasticsearch index
 *
 * @author guregodevo
 */
object EgoElasticDataSource extends ElasticDataSource[ItemEgoData, AuthorVertex, GraphEdge] {


  private def getAuthorReferences(id: JListWrapper[_],
    name: JListWrapper[_],
    activity: JListWrapper[_],
    audience: JListWrapper[_]): Array[MentionData] = {
    val ids = getLongArray(id)
    Option(ids) match {
      case Some(x) => {
        val mentionArray = Array.ofDim[MentionData](x.size)
        val names = getStringArray(name)
        val activities = getLongArray(activity)
        val audiences = getLongArray(audience)
        for (i <- mentionArray.indices) {
          mentionArray(i) = new MentionData(ids(i), names(i), activities(i), audiences(i))
        }
        mentionArray
      }
      case None => null
    }
  }

  override def buildItemInputRDD(sc: SparkContext, parameters: NetworkParameters): RDD[(Long, ItemEgoData)] = {

    val egoParams = new EgoNetworkParameters(parameters)

    val initialAuthorRDD = getRawInputRDD(sc, parameters).map(getItemData)

    //get the connected authors
    val connectedAuthorsRDD = EgoNetwork.getAuthors(initialAuthorRDD)

    //get the k more recently connected authors
    val recentRelatedAuthors = connectedAuthorsRDD.map(_._2).takeOrdered(egoParams.relations)(Ordering[Long].reverse.on(x => x.lastItemDate))

    //build the new connection string
    if (recentRelatedAuthors.size > 0) {

      val newConnection = parameters.connectionString.replaceAll(NICKID_PATTERN, getNickFilter(recentRelatedAuthors, NICK_ID))
        .replaceAll(PARENT_NICKID_PATTERN, getNickFilter(recentRelatedAuthors, PARENT_NICKID))

      val newParameters = new NetworkParameters(ConfigFactory.parseMap(Map(NetworkParameters.CONNECTION_STRING_PROPERTY -> newConnection)).withFallback(parameters.config))

      (initialAuthorRDD ++ getRawInputRDD(sc, newParameters).map(getItemData)).reduceByKey((x, y) => x)
    }
    else
      initialAuthorRDD
  }

  private def getNickFilter(authors: Array[AuthorVertex], nickField: String): String = {
    val nicksFilter = new StringBuilder("{\"terms\": {\"" + nickField + "\": [\"" + authors(0).nickId + "\"")
    for (i <- authors.indices.drop(1))
      nicksFilter ++= ",\"" + authors(i).nickId + "\""
    nicksFilter ++= "]}}"

    return nicksFilter.toString
  }

  override def getItemData(rawObject: Tuple2[String, scala.collection.Map[String, AnyRef]]) = {
    
	def map(key : String) = rawObject._2.getOrElse(key, null)

    val mentions = getAuthorReferences(map(MENTION_ID).asInstanceOf[JListWrapper[_]],
      map(MENTION_NAME).asInstanceOf[JListWrapper[_]],
      map(MENTION_ACTIVITY).asInstanceOf[JListWrapper[_]],
      map(MENTION_AUDIENCE).asInstanceOf[JListWrapper[_]])

    val replies = getAuthorReferences(map(REPLY_ID).asInstanceOf[JListWrapper[_]],
      map(REPLY_NAME).asInstanceOf[JListWrapper[_]],
      map(REPLY_ACTIVITY).asInstanceOf[JListWrapper[_]],
      map(REPLY_AUDIENCE).asInstanceOf[JListWrapper[_]])

    val domains: Array[String] = getStringArray(map(REFERENCE_HOST).asInstanceOf[JListWrapper[scala.collection.Map[String, AnyRef]]])
    val urls: Array[Long] = getLongArray(map(REFERENCE_ID).asInstanceOf[JListWrapper[scala.collection.Map[String, AnyRef]]])

    val item = new ItemEgoData(
      map(ITEM_PARENTID).asInstanceOf[Long],
      getString(map(ITEM_LINK)),
      getString(map(ITEM_BODY)),
      map(ITEM_PUBLISHDATE).asInstanceOf[Long],
      map(PUBLISHER_ID).asInstanceOf[Long],
      getString(map(PUBLISHER_DESCRIPTION)),
      map(NICK_ID).asInstanceOf[Long],
      getString(map(NICK_NAME)),
      map(NICK_ACTIVITY).asInstanceOf[Long],
      map(NICK_RATING).asInstanceOf[Long],
      getString(map(PERSON_LOCATION)),
      getString(map(PERSON_COUNTRY)),
      getString(map(PERSON_WEBSITE)),
      domains,
      urls,
      map(PARENT_PUBLISHDATE).asInstanceOf[Long],
      map(PARENT_NICKID).asInstanceOf[Long],
      getString(map(PARENT_NICKNAME)),
      map(PARENT_NICKACTIVITY).asInstanceOf[Long],
      map(PARENT_NICKRATING).asInstanceOf[Long],
      mentions,
      replies)

    (map(ITEM_ID).asInstanceOf[Long], item)
  }

  final val NICKID_PATTERN = "\\{\\s*\"term\"\\s*:\\s*\\{\\s*\"" + NICK_ID + "\"\\s*:\\s*\"\\d+\"\\s*\\}\\s*\\}"
  final val PARENT_NICKID_PATTERN = "\\{\\s*\"term\"\\s*:\\s*\\{\\s*\"" + PARENT_NICKID + "\"\\s*:\\s*\"\\d+\"\\s*\\}\\s*\\}"

}

class EgoElasticDataSourceFactory extends ServiceFactory[ElasticDataSource[ItemEgoData, AuthorVertex, GraphEdge]] {
  override def service(params: NetworkParameters) = { setNode(params); EgoElasticDataSource }
}

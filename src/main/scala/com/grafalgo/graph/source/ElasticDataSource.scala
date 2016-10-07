package com.grafalgo.graph.source

import scala.collection.convert.Wrappers.JListWrapper
import scala.collection.mutable
import org.apache.spark.SparkContext
import org.elasticsearch.spark._
import com.grafalgo.graph.spi._
import com.grafalgo.graph.spi.GraphDataSource
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.apache.commons.logging.LogFactory
import org.elasticsearch.hadoop.rest.RestService
import scala.collection.JavaConversions._
import scala.collection.convert.decorateAll._
import java.util.ArrayList

/**
 * DataSource corresponding to an elasticsearch index
 *
 * @author guregodevo
 */
trait ElasticDataSource[I, V <: GraphVertex, E <: GraphEntity] extends GraphDataSource[Tuple2[String, scala.collection.Map[String, AnyRef]], I, V, E] {

  import ElasticDataSource._

  override def getRawInputRDD(sc: SparkContext, parameters: NetworkParameters) = {
    val sections = parameters.connectionString.split(CONNECTION_SEPARATOR)
    if (parameters.sampleData) {
      sc.esRDD(sections(0), sections(1), Map("es.scroll.limit" -> (parameters.sampleSize / getShards(sc, sections(0))).toString))
    }
    else
      sc.esRDD(sections(0), sections(1))
  }

  //Avoiding the Option construct intentionally for performance reasons
  protected def getString(value: AnyRef): String = {
    value match {
      case None => null
      case x => x.asInstanceOf[String]
    }
  }

  protected def getLongArray(rawObject: JListWrapper[_]): Array[Long] = {
    Option(rawObject) match {
      case Some(x) => x.underlying.asInstanceOf[ArrayList[Long]].asScala.toArray
      case None => null
    }
  }

  protected def getStringArray(rawObject: JListWrapper[_]): Array[String] = {
    Option(rawObject) match {
      case Some(x) => x.underlying.asInstanceOf[ArrayList[String]].asScala.toArray
      case None => null
    }
  }

  val directLoad = false
}

object ElasticDataSource {

  //FIELD NAMES

  final val ITEM_ID = "item_id"
  final val ITEM_TITLE = "item_title"
  final val ITEM_PARENTID = "item_parentId"
  final val ITEM_LINK = "item_link"
  final val ITEM_BODY = "item_body"
  final val ITEM_PUBLISHDATE = "item_publishDate"
  final val PUBLISHER_ID = "publisher_id"
  final val PUBLISHER_DESCRIPTION = "publisher_description"
  final val NICK_ID = "nick_id"
  final val NICK_NAME = "nick_name"
  final val NICK_ACTIVITY = "item_activity"
  final val NICK_RATING = "item_audience"
  final val PERSON_LOCATION = "person_location"
  final val PERSON_COUNTRY = "person_country"
  final val PERSON_WEBSITE = "person_website"
  final val REFERENCE_ID = "item_referenceIds"
  final val REFERENCE_HOST = "item_site"
  final val REFERENCE_URL = "item_referenceUrls"
  final val PARENT_PUBLISHDATE = "item_parentPublishDate"
  final val PARENT_LINK = "item_parentLink"
  final val PARENT_NICKID = "item_parentNickId"
  final val PARENT_NICKNAME = "item_parentNickName"
  final val PARENT_NICKACTIVITY = "item_parentNickActivity"
  final val PARENT_NICKRATING = "item_parentAudience"
  final val MENTION_ID = "item_mentionNickId";
  final val MENTION_NAME = "item_mentionNickName";
  final val MENTION_ACTIVITY = "item_mentionNickActivity";
  final val MENTION_AUDIENCE = "item_mentionNickAudience";
  final val REPLY_ID = "item_replyNickId";
  final val REPLY_NAME = "item_replyNickName";
  final val REPLY_ACTIVITY = "item_replyNickActivity";
  final val REPLY_AUDIENCE = "item_replyNickAudience";

  final val CONNECTION_SEPARATOR = ";"

  private def getShards(sc: SparkContext, index: String) =
    RestService.findPartitions(new SparkSettingsManager().load(sc.getConf).copy()
      .merge(Map(ConfigurationOptions.ES_RESOURCE_READ -> index)), LogFactory.getLog(this.getClass())).size

  def setNode(parameters: NetworkParameters) {
    if (parameters != null) {
      val sections = parameters.connectionString.split(CONNECTION_SEPARATOR)
      if (sections.length == 3) System.setProperty("es.nodes", sections(0))
    }
  }

}
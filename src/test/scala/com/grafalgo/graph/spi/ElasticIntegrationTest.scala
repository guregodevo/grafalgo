package com.grafalgo.graph.elastic

import scala.beans.BeanProperty
import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.client.Client
import org.elasticsearch.client.Requests
import org.elasticsearch.node.Node
import org.elasticsearch.node.NodeBuilder
import org.junit.AfterClass
import org.junit.BeforeClass
import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.query.QueryBuilders
import scala.beans.BeanProperty
import com.fasterxml.jackson.annotation.JsonProperty
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentBuilder
import com.grafalgo.graph.source.ElasticDataSource
import com.grafalgo.graph.source.ElasticDataSource._
import com.fasterxml.jackson.annotation.JsonInclude.Include

/**
 * Convenience object for enabling an embedded elastic search node on extending test classes.
 * The shipped ElasticsearchIntegrationTest class is not used due to the somewhat cumbersome
 * dependencies and version mayhem.
 *
 * @author dcm
 */
abstract class ElasticIntegrationTest

object ElasticIntegrationTest {

  var es_node: Node = null
  var es_client: Client = null
  final val DEFAULT_INDEX = "test_index"
  final val DEFAULT_TYPE = "test_type"
  final val DEFAULT_INDEX_TYPE = "test_index/test_type"
  final val DATE_PARSER = FastDateFormat.getInstance("dd-MM-yyyy")

  val KEYWORD_SOURCE = "KeywordNetworkElastic"
  val NICK_SOURCE = "NickNetworkElastic"

  private val MAPPER = new ObjectMapper
  MAPPER.setSerializationInclusion(Include.NON_NULL)
  private final val DATA_PATH = "data"

  final val CONNECTION = DEFAULT_INDEX_TYPE + ElasticDataSource.CONNECTION_SEPARATOR + "?q=*:*"

  @BeforeClass
  def prepare {
    removeData
    startNode
  }

  @AfterClass
  def clean {
    stopNode
    removeData
  }

  private def startNode = {
    es_node = NodeBuilder.nodeBuilder.settings(Settings.settingsBuilder.put("path.home", DATA_PATH)).local(true).node
    es_client = es_node.client
  }

  private def stopNode = es_node.close

  private def removeData = FileUtils.deleteDirectory(new File(DATA_PATH))

  def getDate(date: String): Long = DATE_PARSER.parse(date).getTime

  def createIndex(indexName: String, indexType: String) = {
    es_client.admin.indices.prepareCreate(indexName)
      .addMapping(indexType, createMapping(indexType)).execute.actionGet
  }

  def createIndex: CreateIndexResponse = createIndex(DEFAULT_INDEX, DEFAULT_TYPE)

  def deleteIndex(indexName: String) = es_client.admin.indices.delete(Requests.deleteIndexRequest(indexName)).actionGet

  def deleteIndex: DeleteIndexResponse = deleteIndex(DEFAULT_INDEX)

  def insertDocument[T <: Object](indexName: String, indexType: String, documentBean: T) = es_client.index(Requests.indexRequest(indexName).`type`(indexType).
    source(MAPPER.writeValueAsBytes(documentBean)).refresh(true)).actionGet

  def insertDocument[T <: Object](documentBean: T): IndexResponse = insertDocument[T](DEFAULT_INDEX, DEFAULT_TYPE, documentBean)

  def printSampleData(indexName: String, indexType: String) {
    val response = es_client.prepareSearch(indexName).setTypes(indexType).setQuery(QueryBuilders.matchAllQuery).execute.actionGet

    response.getHits.getHits.foreach(x => println(x.getSource))
  }

  def populateSampleItemData(indexName: String, indexType: String) {

    val items = Array(
      new ItemBean(item_id = 1, item_title = "title1", item_body = "body1 #ht1 #ht2 #ht3 keywordA  keywordB", item_link = "link1",
        item_publishDate = getDate("01-06-2015"), nick_id = 1, nick_name = "author1", item_activity = 1, item_audience = 11,
        item_referenceIds = Array(1, 2),
        item_referenceUrls = Array("url1", "url2"),
        item_site = Array("host1", "host1"),
        item_mentionNickId = Array(4),
        item_mentionNickName = Array("author4"),
        item_mentionNickActivity = Array(4),
        item_mentionNickAudience = Array(44),
        item_replyNickId = Array(2),
        item_replyNickName = Array("author2"),
        item_replyNickActivity = Array(3),
        item_replyNickAudience = Array(33)),
      new ItemBean(item_id = 2, item_parentId = 1, item_title = "title2", item_body = "body2 #ht1 #ht2 #ht3 #ht4 #ht5", item_link = "link2",
        item_publishDate = getDate("03-06-2015"), nick_id = 2, nick_name = "author2", item_activity = 2, item_audience = 22,
        item_referenceIds = Array(4, 5),
        item_referenceUrls = Array("url4", "url5"),
        item_site = Array("host3", "host3"),
        item_mentionNickId = Array(4),
        item_mentionNickName = Array("author4"),
        item_mentionNickActivity = Array(4),
        item_mentionNickAudience = Array(44),
        item_replyNickId = Array(1),
        item_replyNickName = Array("author5"),
        item_replyNickActivity = Array(5),
        item_replyNickAudience = Array(55)),
      new ItemBean(item_id = 3, item_parentId = 1, item_title = "title3", item_body = "body3 #ht1 #ht2 #ht3 #ht4 #ht5", item_link = "link3",
        item_publishDate = getDate("04-06-2015"), nick_id = 2, nick_name = "author2", item_activity = 2, item_audience = 22),
      new ItemBean(item_id = 4, item_parentId = 1, item_title = "title4", item_body = "body4 #ht1 #ht2 #ht3 keywordA  keywordB", item_link = "link4",
        item_publishDate = getDate("05-06-2015"), nick_id = 3, nick_name = "author3", item_activity = 3, item_audience = 33),
      new ItemBean(item_id = 5, item_title = "title5", item_body = "body5 #ht1 #ht2 #ht4 #ht5", item_link = "link5",
        item_publishDate = getDate("02-06-2015"), nick_id = 2, nick_name = "author2", item_activity = 2, item_audience = 22),
      new ItemBean(item_id = 6, item_parentId = 5, item_title = "title6", item_body = "body6 #ht1 #ht2", item_link = "link6",
        item_publishDate = getDate("06-06-2015"), nick_id = 3, nick_name = "author3", item_activity = 3, item_audience = 33),
      new ItemBean(item_id = 7, item_parentId = 5, item_title = "title7", item_body = "body7 #ht1 #ht4 #ht5 keyworda keywordC", item_link = "link7",
        item_publishDate = getDate("07-06-2015"), nick_id = 1, nick_name = "author1", item_activity = 1, item_audience = 11,
        item_referenceIds = Array(2, 3),
        item_referenceUrls = Array("url2", "url3"),
        item_site = Array("host1", "host2")),
      new ItemBean(item_id = 8, item_parentId = 4, item_title = "title8", item_body = "body8 #ht4 #ht5 keywordB keywordd", item_link = "link8",
        item_publishDate = getDate("08-06-2015"), nick_id = 2, nick_name = "author2", item_activity = 2, item_audience = 22),
      new ItemBean(item_id = 9, item_parentId = 4, item_title = "title9", item_body = "body9 #ht4 #ht5 keywordE keywordF", item_link = "link9",
        item_publishDate = getDate("09-06-2015"), nick_id = 4, nick_name = "author4", item_activity = 4, item_audience = 44,
        item_referenceIds = Array(3, 2),
        item_referenceUrls = Array("url3", "url2"),
        item_site = Array("host2", "host1")))

    for (item <- items) {
      Option(item.item_parentId).map(value => item.setParent(items(value.intValue() - 1)))
      insertDocument(indexName, indexType, item)
    }

  }

  def populateSampleItemData: Unit = populateSampleItemData(DEFAULT_INDEX, DEFAULT_TYPE)

  def populateDataForSampleTest {

    val item = new ItemBean(item_id = 1, item_title = "title1", item_body = "body1 #ht1 #ht2 #ht3 keywordA  keywordB", item_link = "link1",
      item_publishDate = getDate("01-06-2015"), nick_id = 1, nick_name = "author1", item_activity = 1, item_audience = 11,
      item_referenceIds = Array(1, 0, 2, 0),
      item_referenceUrls = Array("url1", null, "url2", null),
      item_site = Array("host1", null, "host1", null),
      item_mentionNickId = Array(4),
      item_mentionNickName = Array("author4"),
      item_mentionNickActivity = Array(4),
      item_mentionNickAudience = Array(44),
      item_replyNickId = Array(2),
      item_replyNickName = Array("author2"),
      item_replyNickActivity = Array(3),
      item_replyNickAudience = Array(33))

    for (i <- 1 to 100) {
      insertDocument(DEFAULT_INDEX, DEFAULT_TYPE, item)
    }

  }

  def printSampleData: Unit = printSampleData(DEFAULT_INDEX, DEFAULT_TYPE)

  def createMapping(index_type: String) = {
    val builder = XContentFactory.jsonBuilder.startObject.startObject(index_type).startObject("properties")
    addField(ITEM_ID, "long", builder)
    addField(ITEM_TITLE, "string", builder)
    addField(ITEM_PARENTID, "long", builder)
    addField(ITEM_LINK, "string", builder)
    addField(ITEM_BODY, "string", builder)
    addField(ITEM_PUBLISHDATE, "long", builder)
    addField(PUBLISHER_ID, "long", builder)
    addField(PUBLISHER_DESCRIPTION, "string", builder)
    addField(NICK_ID, "long", builder)
    addField(NICK_NAME, "string", builder)
    addField(NICK_ACTIVITY, "long", builder)
    addField(NICK_RATING, "long", builder)
    addField(PERSON_LOCATION, "string", builder)
    addField(PERSON_COUNTRY, "string", builder)
    addField(PERSON_WEBSITE, "string", builder)
    //references
    addField(REFERENCE_ID, "long", builder)
    addField(REFERENCE_HOST, "string", builder)
    addField(REFERENCE_URL, "string", builder)
    //mentions
    addField(MENTION_ID, "long", builder)
    addField(MENTION_NAME, "string", builder)
    addField(MENTION_ACTIVITY, "long", builder)
    addField(MENTION_AUDIENCE, "long", builder)
    //replies
    addField(REPLY_ID, "long", builder)
    addField(REPLY_NAME, "string", builder)
    addField(REPLY_ACTIVITY, "long", builder)
    addField(REPLY_AUDIENCE, "long", builder)
    addField(PARENT_PUBLISHDATE, "long", builder)
    addField(PARENT_LINK, "string", builder)
    addField(PARENT_NICKID, "long", builder)
    addField(PARENT_NICKNAME, "string", builder)
    addField(PARENT_NICKACTIVITY, "long", builder)
    addField(PARENT_NICKRATING, "long", builder)
    builder.endObject.endObject.endObject;

    builder
  }

  private def addField(name: String, typename: String, builder: XContentBuilder) {
    builder.startObject(name)
      .field("type", typename)
      .field("store", "yes")
      .endObject()
  }

}


/**
 * Beans representing the index item data used to build networks.
 * This is assuming a provisional simple elastic index model.
 * Using elastic dynamic mapping.
 *
 * @author dcm
 */
class ItemBean(
    @BeanProperty var item_id: java.lang.Long = null,
    @BeanProperty var item_title: String = null,
    @BeanProperty var item_parentId: java.lang.Long = null,
    @BeanProperty var item_link: String = null,
    @BeanProperty var item_body: String = null,
    @BeanProperty var item_publishDate: java.lang.Long = null,
    @BeanProperty var publisher_id: java.lang.Long = 11,
    @BeanProperty var publisher_description: String = "Twitter",
    @BeanProperty var nick_id: java.lang.Long = null,
    @BeanProperty var nick_name: String = null,
    @BeanProperty var item_activity: java.lang.Integer = null,
    @BeanProperty var item_audience: java.lang.Integer = null,
    @BeanProperty var person_location: String = null,
    @BeanProperty var person_country: String = null,
    @BeanProperty var person_website: String = null,
    @BeanProperty var item_referenceIds: Array[Long] = null,
    @BeanProperty var item_referenceUrls: Array[String] = null,
    @BeanProperty var item_site: Array[String] = null,
    @BeanProperty var item_replyNickId: Array[Long] = null,
    @BeanProperty var item_replyNickName: Array[String] = null,
    @BeanProperty var item_replyNickAudience: Array[Long] = null,
    @BeanProperty var item_replyNickActivity: Array[Long] = null,
    @BeanProperty var item_mentionNickId: Array[Long] = null,
    @BeanProperty var item_mentionNickName: Array[String] = null,
    @BeanProperty var item_mentionNickAudience: Array[Long] = null,
    @BeanProperty var item_mentionNickActivity: Array[Long] = null,
    @BeanProperty var item_parentPublishDate: java.lang.Long = null,
    @BeanProperty var item_parentLink: String = null,
    @BeanProperty var item_parentNickId: java.lang.Long = null,
    @BeanProperty var item_parentNickName: String = null,
    @BeanProperty var item_parentNickActivity: java.lang.Integer = null,
    @BeanProperty var item_parentNickAudience: java.lang.Integer = null) {

  def setParent(parentItem: ItemBean) {
    item_parentId = parentItem.item_id
    item_parentPublishDate = parentItem.item_publishDate
    item_parentLink = parentItem.item_link
    item_parentNickId = parentItem.nick_id
    item_parentNickName = parentItem.nick_name
    item_parentNickActivity = parentItem.item_activity
    item_parentNickAudience = parentItem.item_audience
  }
}

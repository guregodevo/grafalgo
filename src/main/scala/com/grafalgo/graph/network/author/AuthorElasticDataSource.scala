package com.grafalgo.graph.network.author

import scala.collection.convert.Wrappers.JListWrapper
import scala.collection.mutable
import org.apache.spark.SparkContext
import org.elasticsearch.spark.sparkContextFunctions
import com.grafalgo.graph.spi._
import com.grafalgo.graph.spi.GraphDataSource
import com.grafalgo.graph.source.ElasticDataSource
import com.grafalgo.graph.source.ElasticDataSource._

//TODO This has to be reviewed with the definitive elasticsearch mapping. 
/**
 * DataSource corresponding to an elasticsearch index
 *
 * @author guregodevo
 */
object AuthorElasticDataSource extends ElasticDataSource[ItemAuthorData, AuthorVertex, AuthorEdge] {

  override def getItemData(rawObject: Tuple2[String, scala.collection.Map[String, AnyRef]]) :(Long, ItemAuthorData) = {
   
    def map(key : String) = rawObject._2.getOrElse(key, null)

    val domains: Array[String] = getStringArray(map(REFERENCE_HOST).asInstanceOf[JListWrapper[_]])
    val urls: Array[Long] = getLongArray(map(REFERENCE_ID).asInstanceOf[JListWrapper[_]])

    val item = new ItemAuthorData(
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
      map(PARENT_NICKRATING).asInstanceOf[Long])

    (map(ITEM_ID).asInstanceOf[Long], item)
  }

}

class AuthorElasticDataSourceFactory extends ServiceFactory[ElasticDataSource[ItemAuthorData, AuthorVertex, AuthorEdge]] {
  override def service(params:NetworkParameters) ={setNode(params); AuthorElasticDataSource}
}

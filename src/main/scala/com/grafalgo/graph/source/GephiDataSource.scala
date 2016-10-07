package com.grafalgo.graph.source

import com.grafalgo.graph.spi.GraphDataSource
import org.apache.spark.SparkContext
import com.grafalgo.graph.spi.GraphEntity
import com.grafalgo.graph.spi.util.spark.XmlInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import com.grafalgo.graph.spi.GraphVertex
import scala.xml.XML
import scala.xml.Elem
import scala.collection.mutable
import com.grafalgo.graph.spi.GraphMetrics._
import com.grafalgo.graph.spi.GraphMetrics
import com.grafalgo.graph.spi.ScalarMetricProcessor
import com.grafalgo.graph.spi.NetworkParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Edge
import com.grafalgo.graph.spi.util.spark.XmlInputFormat
import com.grafalgo.graph.spi.MetricProcessor
import com.grafalgo.graph.spi.util.spark.XmlInputFormat
import java.text.SimpleDateFormat
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.commons.lang3.time.FastDateFormat._

/**
 * DataSource corresponding to a gephi file
 *
 * @author guregodevo
 */
trait GephiDataSource[I, V <: GraphVertex, E <: GraphEntity] extends GraphDataSource[Tuple2[LongWritable, Text], I, V, E] {
  import GephiDataSource._

  override def getRawInputRDD(sc: SparkContext, parameters: NetworkParameters) = {
    sc.hadoopConfiguration.set(XmlInputFormat.TAG_KEY, TAG_KEY)
    sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "utf-8")

    //Read xml
    sc.newAPIHadoopFile(
      parameters.connectionString,
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text])
  }

  /**
   * Load the vertices and edges RDDs from the source file. The metrics loaded from the file are combined with the ones requested and also returned
   */
  override def loadGraphRDDs(sc: SparkContext, parameters: NetworkParameters): (RDD[(Long, V)], RDD[Edge[E]], Array[GraphMetric]) = {
    val source = getRawInputRDD(sc, parameters).map(_._2.toString).cache

    val nodeBlocks = source.filter(_.startsWith(NODE_TAG_PREFIX)).cache
    
    val currentMetrics = parseMetrics(nodeBlocks.take(1)(0))

    val processors = for (metric <- currentMetrics) yield (metric.getMetricProcessor)

    //parse xml to generate nodes 
    val nodes = nodeBlocks.map(parseNode(_, processors))

    nodeBlocks.unpersist(false)

    //parse xml to generate edges
    val edges = source.filter(_.startsWith(EDGE_TAG_PREFIX)).map(parseEdge)

    source.unpersist(false)

    (nodes, edges,  (currentMetrics ++ parameters.requestedMetrics).toSet.toArray)
  }

  protected def parseNode(xmlBlock: String, metrics: Array[MetricProcessor[_, GraphVertex]]): (Long, V)

  protected def parseEdge(xmlElem: String): Edge[E]

  override val directLoad = true

  protected def parseMetrics(xmlElem: String): Array[GraphMetric] = {

    val nodeElem = XML.loadString(xmlElem.toString)
  
    val attrNames = (for( x <- (nodeElem \ ATTRIBUTES_ELEM \ ATTRIBUTE_ELEM)) yield ((x \ KEY_ATTR).toString)).toArray
    
    for (attrName <- attrNames; metricName = attrName.toString;
      if (metricNames.contains(metricName))
    ) yield { GraphMetrics.withName(metricName) }
  }

  protected def getAttributes(elem: Elem): mutable.Map[String, String] = {
    val result = mutable.Map[String, String]()
    (elem \ ATTRIBUTES_ELEM \ ATTRIBUTE_ELEM).foreach { elem => result += (((elem \ KEY_ATTR).toString, (elem \ VALUE_ATTR).toString)) }

    result
  }

  protected def getPairValues(value: String): (String, String) = {
    value match {
      case null => (null, null)
      case "" => (null, null)
      case x => { val pair = x.split(ITEM_SEPARATOR); if (pair.length == 1) (pair(1), null) else (pair(1), pair(2)) }
    }
  }

  def parseGephiDate(date: String) = {
    GEPHI_DATE_FORMAT.parse(date).getTime
  }

}

object GephiDataSource {
  final val NODE_TAG = "node"
  final val EDGE_TAG = "edge"

  final val NODE_TAG_PREFIX = "<n"
  final val EDGE_TAG_PREFIX = "<e"

  final val LABEL_ATTR = "@label"
  final val SOURCE_ATTR = "@source"
  final val TARGET_ATTR = "@target"
  final val WEIGHT_ATTR = "@weight"
  final val ID_ATTR = "@id"
  final val KEY_ATTR = "@for"
  final val VALUE_ATTR = "@value"

  final val ATTRIBUTES_ELEM = "attvalues"
  final val ATTRIBUTE_ELEM = "attvalue"

  final val ITEM_SEPARATOR = "||"

  final val TAG_KEY = NODE_TAG + "," + EDGE_TAG

  final val GEPHI_DATE_FORMAT_DEFINITION = "dd/MM/yyyy hh:mm";
  final val GEPHI_DATE_FORMAT = FastDateFormat.getInstance(GEPHI_DATE_FORMAT_DEFINITION)
  
  val metricNames = for (m <- GraphMetrics.values) yield m.toString
}
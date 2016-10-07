package com.grafalgo.graph.spi

import scala.collection.JavaConversions._
import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.Map
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigObject
import GraphMetrics._
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import com.typesafe.config.ConfigException

/*
 * Class containing the common config parameters for the network jobs
 */
class NetworkParameters(jobConfig: Config) {

  import NetworkParameters._

  val scf = System.getProperty("spark.config.file")
  if (scf != null) System.setProperty("config.file", scf)

  val config = jobConfig.withFallback(ConfigFactory.systemProperties).withFallback(ConfigFactory.load()).withFallback(getDefaultConfig)

  def this() = this(ConfigFactory.empty)

  def this(params: NetworkParameters) = this(if (params == null) ConfigFactory.empty else params.config)

  protected def getDefaultConfig = ConfigFactory.parseMap(mapAsJavaMap(
    Map(SOURCE_PROPERTY -> GraphDataSource.getDefaultServiceName,
      EXPORTER_PROPERTY -> GraphExporter.getDefaultServiceName,
      PARTITION_STRATEGY_PROPERTY -> "CanonicalRandomVertexCut",
      PARTITION_NUMBER_PROPERTY -> "0",
      POWER_ITERATIONS -> "100",
      RANK_ITERATIONS -> "100",
      RANK_RESET_PROBABILITY -> "0.15",
      RANK_TOLERANCE -> "0.01",
      EXTRACT_GIANT -> "false",
      SAMPLE_DATA -> "false",
      SAMPLE_SIZE -> "20000",
      METRIC_LIST -> "",
      MODULARITY_USE_WEIGHT -> "true",
      MODULARITY_MIN_UPDATES -> "1",
      MODULARITY_RESOLUTION -> "1.0",
      MODULARITY_RESOLUTION_INCREASE -> "0.001",
      MODULARITY_ITERATIONS -> "30",
      BETWEENNESS_BATCHSIZE -> "16",
      BETWEENNESS_MAX_PIVOTS -> "64",
      REMOVE_UNCONNECTED -> "true",
      FILTERS + ".0.name" -> null)))

  override def toString = {
    val result: StringBuilder = new StringBuilder("\nCurrent configuration:")
    asScalaSet(config.withOnlyPath(ROOT).entrySet).foreach(x => result ++= ("\n" + x.getKey + "=" + x.getValue.unwrapped.toString))

    result.toString
  }

  //with fallback
  val source = config.getString(SOURCE_PROPERTY)
  val exporter = config.getString(EXPORTER_PROPERTY)
  val partitionStrategy = getPartitionStrategy(config.getString(PARTITION_STRATEGY_PROPERTY))
  val partitionNumber = config.getInt(PARTITION_NUMBER_PROPERTY)
  val eigenCentralityIterations = config.getInt(POWER_ITERATIONS)
  val pageRankIterations = config.getInt(RANK_ITERATIONS)
  val pageRankResetProb = config.getDouble(RANK_RESET_PROBABILITY)
  val pageRankTolerance = config.getDouble(RANK_TOLERANCE)  
  val extractGiant = config.getBoolean(EXTRACT_GIANT)
  val sampleData = config.getBoolean(SAMPLE_DATA)
  val sampleSize = config.getInt(SAMPLE_SIZE)
  val modularityUseWeight = config.getBoolean(MODULARITY_USE_WEIGHT)
  val modularityMinUpdates = config.getInt(MODULARITY_MIN_UPDATES)  
  val modularityResolutionIncrease = config.getDouble(MODULARITY_RESOLUTION_INCREASE)
  val modularityResolution = config.getDouble(MODULARITY_RESOLUTION)
  val modularityIterations = config.getInt(MODULARITY_ITERATIONS)
  val removeUnconnected = config.getBoolean(REMOVE_UNCONNECTED)
  val betweennessBatchSize = config.getInt(BETWEENNESS_BATCHSIZE)
  val betweennessMaxPivots = config.getInt(BETWEENNESS_MAX_PIVOTS)

  protected def getMetrics = {
    var metricList = config.getString(METRIC_LIST).toUpperCase
    metricList = if (metricList.equals("ALL")) ALL_METRICS else metricList
    for (metricName <- if (metricList.trim.equals("")) Array[String]() else metricList.split("\\s*,\\s*").toSet.toArray)
      yield (try { GraphMetrics.withName(metricName) } catch { case ex: NoSuchElementException => throw new UnsupportedOperationException("Metric " + metricName + " is not defined") })
  }

  //Metrics
  val metrics: Array[GraphMetric] = getMetrics

  val requestedMetrics = (for (metric <- metrics; m <- metric.getAssociatedMetrics) yield m).toSet.toArray

  //Filters
  //TODO review this logic
  val filters = {
    val result =
      for {
        c <- config.getObjectList(FILTERS).asScala
        val filter = c.get("name")
        val minObject = c.get("min")
        val maxObject = c.get("max")
        if (filter != null && filter.unwrapped() != null)
      } yield (try {
        val metricFilter = GraphMetrics.withName(filter.unwrapped.toString.toUpperCase)
        val requestedMetrics = this.requestedMetrics
        metricFilter.getFilter(if (minObject == null || minObject.unwrapped == null) null else minObject.unwrapped.toString,
          if (maxObject == null || maxObject.unwrapped == null) null else maxObject.unwrapped.toString)
      }
      catch {
        case ex: NoSuchElementException => throw new UnsupportedOperationException("Filter " + filter.unwrapped.toString.toUpperCase + " is not defined")
      })

    result.toSet.toArray
  }

  //Mandatory
  val network = config.getString(NETWORK_PROPERTY)
  val connectionString = config.getString(CONNECTION_STRING_PROPERTY)
  val outputPath = config.getString(OUTPUTPATH_PROPERTY)

}

object NetworkParameters extends NetworkParameters {

  final val ROOT = "network"

  final val NETWORK_PROPERTY = ROOT + ".type"
  final val CONNECTION_STRING_PROPERTY = ROOT + ".connectionString";
  final val OUTPUTPATH_PROPERTY = ROOT + ".outputPath"
  final val POWER_ITERATIONS = ROOT + ".metric.eigencentrality.iterations"
  final val RANK_ITERATIONS = ROOT + ".metric.pagerank.iterations"
  final val RANK_RESET_PROBABILITY = ROOT + ".metric.pagerank.resetProb"
  final val RANK_TOLERANCE = ROOT + ".metric.pagerank.tolerance"
  final val EXTRACT_GIANT = ROOT + ".extractGiantComponent"
  final val SAMPLE_DATA = ROOT + ".sampleData"
  final val SAMPLE_SIZE = ROOT + ".sampleSize"
  final val MODULARITY_ITERATIONS = ROOT + ".metric.modularity.iterations" 
  final val MODULARITY_USE_WEIGHT = ROOT + ".metric.modularity.useweight"
  final val MODULARITY_RESOLUTION = ROOT + ".metric.modularity.resolution"
  final val MODULARITY_RESOLUTION_INCREASE = ROOT + ".metric.modularity.increase"
  final val MODULARITY_MIN_UPDATES = ROOT + ".metric.modularity.min.updates"
  final val MODULARITY_IMPL = ROOT + ".metric.modularity.impl"
  final val REMOVE_UNCONNECTED = ROOT + ".removeUnconnected"
  final val BETWEENNESS_BATCHSIZE = ROOT + ".metric.betweenness.batchsize"
  final val BETWEENNESS_MAX_PIVOTS = ROOT + ".metric.betweenness.maxpivots"
  

  final val SOURCE_PROPERTY = ROOT + ".datasource"
  final val EXPORTER_PROPERTY = ROOT + ".exporter"
  final val PARTITION_STRATEGY_PROPERTY = ROOT + ".partition.strategy"
  final val PARTITION_NUMBER_PROPERTY = ROOT + ".partition.number"
  
  //Metrics
  final val METRIC_LIST = ROOT + ".metrics"

  //Filters
  final val FILTERS = ROOT + ".filter"
  
  final val ALL_METRICS = "MODULARITY,EIGENCENTRALITY,WEIGHTEDDEGREE,COMPONENT,PAGERANK";

}
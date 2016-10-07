package com.grafalgo.graph.exporter

import scala.collection.mutable.ArrayBuffer
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import com.grafalgo.graph.spi._
import org.apache.spark.rdd.RDD
import com.grafalgo.graph.spi.MetricProcessor
import com.grafalgo.graph.spi.GraphMetrics
import java.text.SimpleDateFormat
import java.util.Date
import java.io.File
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import com.grafalgo.graph.spi.util.scala.LazyLogging
import scala.reflect.ClassTag

/**
 * Gephi CSV Exporter
 *
 * object implementing the GraphExporter trait for Gephi CSV format
 *
 * @author gdx
 */
trait CsvExporter[V <: GraphVertex, E <: GraphEntity] extends GraphExporter[V, E] {

  val SEPARATOR = ";"

  final val DIRECTED = "Directed"
  final val UNDIRECTED = "Undirected"

  private val EDGE_SUFFIX = "_edges"
  private val NODE_SUFFIX = "_nodes"

  override def getFormatLabel = "csv"

  override def getGraphAsString(sc: SparkContext, graph: Graph[V, E], processors: Array[MetricProcessor[_, GraphVertex]]) = throw new UnsupportedOperationException()

  private def getVertexMetricString(x:V, processors: Array[MetricProcessor[_, GraphVertex]]):String = SEPARATOR + processors.map(p => opt(p.get(x))).mkString(SEPARATOR)
    

  def getEdgesAsString(sc: SparkContext, graph: Graph[V, E]): RDD[String] = {
    sc.parallelize(Array(getEdgeHeader().mkString(SEPARATOR))) ++ graph.edges.zipWithUniqueId().map({ case (x, i) => getEdgeAsString(x, i) })
  }

  def getVerticesAsString(sc: SparkContext, graph: Graph[V, E], processors: Array[MetricProcessor[_, GraphVertex]]): RDD[String] = {
    sc.parallelize(Array((getVertexHeader() ++ processors.map(p => p.name)).mkString(";"))) ++ graph.vertices.map(x => getVertexAsString(x, processors) + getVertexMetricString(x._2, processors))
  }

  def getEdgeAsString(edge: Edge[E]): String = {
    throw new UnsupportedOperationException("Use getEdge passing the edge id instead")
  }

  def getEdgeAsString(edge: Edge[E], i: Long): String

  def getEdgeHeader(): Array[String]

  def getVertexHeader(): Array[String]

  //null safe value
  protected def opt[T](o: T)(implicit m: ClassTag[T]): String = {
    o match {
      case null => ""
      case s: String => s
      case s: Any => s.toString
    }
  }

  /*
   * Save Graph nodes and edges RDDs to separate files.  
   */
  override def saveGraph(sc: SparkContext, graph: Graph[V, E], processors: Array[MetricProcessor[_, GraphVertex]], outputPath: String, jobName: String) {
    saveCSV(sc, processors, outputPath, jobName, NODE_SUFFIX, getVerticesAsString(sc, graph, processors))
    saveCSV(sc, processors, outputPath, jobName, EDGE_SUFFIX, getEdgesAsString(sc, graph))
  }

  /*
   * Save string RDD to file. Defaults to one file 
   */
  def saveCSV(sc: SparkContext, processors: Array[MetricProcessor[_, GraphVertex]], outputPath: String, jobName: String, suffix: String, rdd: RDD[String]) {
    val tempFolder = new Path(outputPath + File.separator + jobName + TEMP_SUFFIX)
    val fs: FileSystem = FileSystem.get(tempFolder.toUri(), new org.apache.hadoop.conf.Configuration())
    fs.delete(tempFolder, true)
    rdd.saveAsTextFile(tempFolder.toString)
    val filename = outputPath + File.separator + jobName + suffix + "." + getFormatLabel
    mergeOutputFiles(tempFolder.toString, filename)
    fs.delete(tempFolder, true)
    logger.info("Output file: " + filename)
  }
}
package com.grafalgo.graph.spi

import java.io.File
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import com.grafalgo.graph.spi.util.scala.LazyLogging

/**
 * Trait for saving to disk a String representation of the graph
 *
 * @author guregodevo
 */
trait GraphExporter[V <: GraphVertex, E] extends Serializable with LazyLogging{

  /*
   * Return the string representation of the graph
   */
  def getGraphAsString(sc: SparkContext, graph: Graph[V, E], processors:Array[MetricProcessor[_,GraphVertex]]): RDD[String]  

  def getVertexAsString(vertex: Tuple2[VertexId, V], processors: Array[MetricProcessor[_,GraphVertex]]):String
   
  def getEdgeAsString(edge: Edge[E]):String
  
  /*
   * Return a label identifying the exporting format
   */
  def getFormatLabel: String

  /*
   * Save string RDD to file. Defaults to one file 
   */
  def saveGraph(sc: SparkContext, graph: Graph[V, E], processors: Array[MetricProcessor[_,GraphVertex]], outputPath: String, jobName:String) {
    val tempFolder = new Path(outputPath + File.separator + jobName + TEMP_SUFFIX)
    val fs: FileSystem = FileSystem.get(tempFolder.toUri(), new org.apache.hadoop.conf.Configuration())
    fs.delete(tempFolder, true)
    getGraphAsString(sc, graph, processors).saveAsTextFile(tempFolder.toString)
    mergeOutputFiles(tempFolder.toString, outputPath + File.separator + jobName + "." + getFormatLabel)
    fs.delete(tempFolder, true)
    logger.info("Output file: " + outputPath + File.separator + jobName + "." + getFormatLabel)
  }

}

/*
 * Companion Factory Provider
 */
object GraphExporter extends ServiceProvider[GraphExporter[_, _]] {
  override lazy val fileName = "job-service"
  override lazy val serviceRoot = "exporter"

  override def getDefaultServiceName = configObject.getString("defaultExporter")
}



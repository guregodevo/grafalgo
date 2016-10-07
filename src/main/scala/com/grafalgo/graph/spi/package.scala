package com.grafalgo.graph

import java.lang.annotation.Annotation
import java.lang.reflect.Method
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.graphx.Graph
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path


import org.apache.spark.graphx.PartitionStrategy._


package object spi { 
  
  val ITEM_SEPARATOR = "||"
  val TEMP_SUFFIX = "_temp"
  val REDUCER_OUTPUT_PREFIX = "part-"
  val HDFS_SCHEME = "hdfs"

  def getPartitionStrategy(value: String) = {
    Option(value) match {
      case Some("CanonicalRandomVertexCut") => CanonicalRandomVertexCut
      case Some("RandomVertexCut") => RandomVertexCut
      case Some("EdgePartition1D") => EdgePartition1D
      case Some("EdgePartition2D") => EdgePartition2D
      case _ => CanonicalRandomVertexCut
    }
  }

  /*
   * Merge the distributed output files to an unique result file. All files
   * are assumed to be located in the same file system scheme
   */
  def mergeOutputFiles(inputFolder: String, outputFile: String) {
    val iPath = new Path(inputFolder)
    var fs: FileSystem = FileSystem.get(iPath.toUri(), new org.apache.hadoop.conf.Configuration())
    val oFile = new Path(outputFile)

    fs.delete(oFile, false)

   //Concat api seems quite restrictive at the moment, it requires target file block to be full, we cannot guarantee that, defaulting to copymerge 
//    if (fs.getUri.getScheme.equals(HDFS_SCHEME))
//      //Experimental: use new HDFS concat api to link the file blocks directly
//      fs.concat(oFile, fs.listFiles(iPath, false).filter(_.getPath.getName.startsWith(REDUCER_OUTPUT_PREFIX)).toList
//        .sortBy(_.getPath.getName).map(_.getPath).toArray)
//    else
      FileUtil.copyMerge(fs, iPath, fs, oFile, true, new org.apache.hadoop.conf.Configuration(), null)
  }

  /*
   * Return the statistics of the last DAG execution
   */
  def getJobSummaryInfo(sc: SparkContext) = {
    val builder = new StringBuilder("\nJob Statistics:") ++= "\nUser: " ++= sc.sparkUser ++=
      "\nJob: " ++= sc.appName ++= "\nTotal time: " ++= (System.currentTimeMillis - sc.startTime) +
      " millis"++= "\nRDD statistics:\n "
      sc.getRDDStorageInfo.foreach { builder ++= _ + "\n" }

    builder.toString
  }

  /*
   * Return the graph information including the RDD transformations lineage
   */
  def getGraphInfo(graph: Graph[_, _]) = {
    val builder = new StringBuilder("\nGraph Summary:") ++= "\nVertices: " ++= graph.vertices.count + "" ++= "\nEdges: " ++=
      graph.edges.count + "" ++= "\nVertices Lineage:\n" ++= graph.vertices.toDebugString ++= "\nEdges Lineage:\n" ++=
      graph.edges.toDebugString

    builder.toString
  }

  /*
   * Decorator to get a real Iterator from the RemoteIterator 
   */
  implicit class RemoteIterator[E](it: org.apache.hadoop.fs.RemoteIterator[E]) extends Iterator[E] {
    override def next() = it.next()
    override def hasNext() = it.hasNext()
  }


}
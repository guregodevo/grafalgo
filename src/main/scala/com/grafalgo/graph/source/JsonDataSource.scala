package com.grafalgo.graph.source

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

import com.grafalgo.graph.spi.GraphDataSource
import com.grafalgo.graph.spi.GraphEntity
import com.grafalgo.graph.spi.ScalarMetricProcessor
import com.grafalgo.graph.spi.GraphMetrics.GraphMetric
import com.grafalgo.graph.spi.GraphVertex
import com.grafalgo.graph.spi.NetworkParameters

/**
 * DataSource corresponding to a json file
 *
 * @author guregodevo
 */
trait JsonDataSource[I, V <: GraphVertex, E <: GraphEntity] extends GraphDataSource[Tuple2[LongWritable, Text], I, V, E] {
  

  override def getRawInputRDD(sc: SparkContext, parameters: NetworkParameters) = {

    //Read xml
    sc.newAPIHadoopFile(
      parameters.connectionString,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text])
  }

  /**
   * Load the vertices and edges RDDs from the source file. The metrics loaded from the file are combined with the ones requested and also returned
   */
  override def loadGraphRDDs(sc: SparkContext, parameters: NetworkParameters): (RDD[(Long, V)], RDD[Edge[E]], Array[GraphMetric]) = {
    val source = getRawInputRDD(sc, parameters).map(_._2.toString).cache

    val nodes = source.map(parseVertex).filter(_ != null)

    val edges = source.map(parseEdge).filter(_ != null)

    source.unpersist(false)

    (nodes, edges, Array[GraphMetric]()) //TODO load GraphMetric when needed
  }


  def parseVertex(v: String): (Long, V)
  
  def parseEdge(v: String): Edge[E] 
  
  def processVertex(v: (Long, V), processors: Array[ScalarMetricProcessor[_]]): (Long, V) = {
    processors.foreach(processor => processor.putString(processor.name, v._2))
    v
  }


  override val directLoad = true




}


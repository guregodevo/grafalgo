package com.grafalgo.graph.spi

import java.io.BufferedWriter
import java.io.File
import java.io.OutputStreamWriter

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._

import GraphMetrics.GraphMetric
import GraphMetrics.MetricDecorator
import GraphMetrics.MetricSlots
import GraphMetrics.toMetric
import com.grafalgo.graph.spi.ScalarProcessor.BINS
import com.grafalgo.graph.spi.util.scala.AutoResource.Arm
import com.grafalgo.graph.spi.util.scala.LazyLogging
import com.grafalgo.graph.spi.util.spark.GraphXKryo

/**
 * Trait that defines the common api for the Networks,the objects corresponds to the application entry point
 * and the jobs factory
 *
 * The jobs are currently modeled as singleton objects. The only state kept is the spark context,
 * shared across all the jobs and a configuration object containing the job parameters.
 *
 * This context is unique per class loader (@see <a href=https://issues.apache.org/jira/browse/SPARK-2243">SPARK-2243</a>
 *
 * As per design decision the context is not immutable meaning that it can be stopped and instantiated again using the api.
 * The rationale being:
 *
 * Some vendor dependent extensions like es-hadoop force to change the configuration options to connect to a different cluster.
 * Facilitate the testing in the same jvm
 *
 * T target object the raw input was mapped to
 * V vertex object with the vertex properties for the network
 * E edge object with the edges properties for the network
 *
 * @author guregodevo
 *
 */
trait NetworkJob[T, V <: GraphVertex, E <: GraphEdge] extends LazyLogging with AutoCloseable {

  protected val parameters: NetworkParameters

  //Must be shared across all the jobs
  @volatile private var sc: SparkContext = _

  /*
   * Creates the graph, apply the metrics and exports results to a file using the configured exporter
   */
  def buildNetwork(implicit tag: ClassTag[V], etag: ClassTag[E]) {

    logger.info("\nNetwork parameters:" + parameters)

    //Validate parameters
    validateNetwork()

    //Graph creation
    logger.info("Building the graph " + jobName)
    val start = System.nanoTime
    val (graph, metrics) = buildTheGraph
    graph.cache
    graph.edges.foreachPartition { x => None }

    logger.info("Time consumed (ms): " + (System.nanoTime - start) / 1000000)

    //Graph processing
    val processedGraph = applyFilters(computeMetrics(graph.extractGiantComponent(parameters))).cache

    logger.info("Job " + jobName + " finished. Graph stats: " + processedGraph.vertices.count + " vertices, " + processedGraph.edges.count + " edges.")

    //Export statistics
    exportStatistics(processedGraph, metrics)
    //Export to file
    val exporter: GraphExporter[V, E] = GraphExporter(getExporter, parameters)
    exporter.saveGraph(context, processedGraph, metrics.getProcessors, parameters.outputPath, jobName)

    //Cleanup
    processedGraph.unpersistVertices(false)
    processedGraph.edges.unpersist(false)

    logger.info(getJobSummaryInfo(context))
  }

  /**
   * Returns the graph created from a repository either processing a dataset of documents of retrieving the stored vertices and edges
   *
   * Returns the partitioned graph plus the set of metrics (requested + already stored if any)
   */
  private[spi] def buildTheGraph(implicit tag: ClassTag[V], etag: ClassTag[E]): (Graph[V, E], MetricSlots) = {
    val source = GraphDataSource[GraphDataSource[_, T, V, E]](getSource,parameters)
    if (source.directLoad) { val result = source.loadGraphRDDs(context, parameters); (buildGraph(result._1, result._2), new MetricSlots(result._3, isDirected)) }
    else {
      val inputRDD = (if (parameters.partitionNumber == 0) buildInputRDD.cache else buildInputRDD.repartition(parameters.partitionNumber)).cache
      logger.info("Number of documents in input dataset: {}", inputRDD.count + "")
      val result = (buildGraph(inputRDD), new MetricSlots(parameters.requestedMetrics, isDirected))
      inputRDD.unpersist(false)

      result
    }
  }

  /**
   * Remove the isolated nodes, not members of edges
   */
  protected def removeUnconnectedNodes(vertices: RDD[Tuple2[VertexId, V]], edges: RDD[Edge[E]])(implicit tag: ClassTag[V], etag: ClassTag[E]) = {
    
    if (parameters.removeUnconnected) {
      edges.cache
      val nodesConnected = (edges.map(x => (x.srcId, null)) union edges.map(x => (x.dstId, null))).distinct
      vertices.leftOuterJoin(nodesConnected).filter(_._2._2.isDefined).mapValues(_._1)
    }
    else
      vertices
  }

  /**
   * Calculate and export to file summary statistics, histogram and percentiles for all present metrics
   */
  private def exportStatistics(graph: Graph[V, E], metrics: MetricSlots)(implicit tag: ClassTag[V]) {

    if (metrics.slots.length > 0) {
      val start = System.nanoTime
      logger.info("Generating statistic summary for the metrics")
      val statFile = new Path(parameters.outputPath + File.separator + jobName + "." + "stats")
      for (
        dos <- statFile.getFileSystem(new org.apache.hadoop.conf.Configuration()).create(statFile);
        osw <- new OutputStreamWriter(dos); bw <- new BufferedWriter(osw)
      ) {

        val scalarProcessors = ArrayBuffer[ScalarMetricProcessor[_]]()
        val clusterProcessors = ArrayBuffer[ClusterMetricProcessor[_]]()
        metrics.getProcessors.foreach(x => x match {
          case s: ScalarMetricProcessor[_] => scalarProcessors += s
          case c: ClusterMetricProcessor[_] => clusterProcessors += c
        })

        //Uses mllib optimized numeric library
        val processors = scalarProcessors.toArray
        val stats: MultivariateStatisticalSummary = if (processors.size > 0) Statistics.colStats(graph.vertices
          .map(node => Vectors.dense(for { p <- processors } yield (p.getAsDouble(node._2)))))
        else null
        var index = 0
        for (processor <- scalarProcessors) {

          bw.write("Stats for metric " + processor.name + ":\n")
          bw.write("Count: " + stats.count + "\n")
          bw.write("Average: " + stats.mean(index) + "\n")
          bw.write("Max: " + stats.max(index) + "\n")
          bw.write("Min: " + stats.min(index) + "\n")
          bw.write("Variance: " + stats.variance(index) + "\n")
          bw.write("Non zeros: " + stats.numNonzeros(index) + "\n")
          
          //Top 25 Nodes for the metric:          
 
          val exporter: GraphExporter[V, E] = GraphExporter(getExporter)
          bw.write("Top Nodes\n")
          processor.getTop(graph.vertices,25).foreach(n => bw.write(exporter.getVertexAsString(n, metrics.getProcessors) +"\n"))

          //Our strict percentiles implementation
          val (percentiles, minOuterFence, maxOuterFence) = processor.getPercentiles(graph.vertices.map(_._2))
          bw.write("Percentiles: " + percentiles.mkString(",") + "\n")

          var max = if (minOuterFence == maxOuterFence) stats.max(index) else maxOuterFence
          var min = if (minOuterFence == maxOuterFence) stats.min(index) else minOuterFence

          //DoubleRDD histogram implementation with accumulators, values outside the extreme range (3 x interquartile range) are ignored
          bw.write("Histogram, range: " + (max - min) / BINS + ", min:" + min + ", max:" + max + ", bins: " +
            processor.getHistogram(graph.vertices.map(_._2), min, max).mkString(",") + "\n")
          index += 1
        }

        for (processor <- clusterProcessors) {
          bw.write("Stats for metric " + processor.name + ":\n")
          val (count, clusters) = processor.getTopClusters(graph.vertices.map(_._2))
          bw.write("Number of Clusters:\n")
          bw.write(count.toString)
          bw.write("\nTop Clusters:\n")
          for (c <- clusters) {
            bw.write("Id: " + c._1 + " Size: " + c._2 + "\n")
          }
        }
      }
      logger.info("Time consumed (ms): " + (System.nanoTime - start) / 1000000)
    }

  }

  /*
   * Creates a spark context
   */
  protected[spi] def context: SparkContext = {
    if (sc == null) this.synchronized { if (sc == null) sc = new SparkContext(getDefaultConf.setAppName(jobName)) }
    sc
  }

  /*
   * Set external context
   */
  def setContext(scp: SparkContext) {
    if (scp != null) this.synchronized { sc = scp }
  }

  /*
   * Builds the input items RDD
   */
  protected[spi] def buildInputRDD: RDD[(Long, T)] = GraphDataSource[GraphDataSource[_, T, V, E]](getSource, parameters)
    .buildItemInputRDD(context, parameters)

  /*
   * Returns a graph given the input RDD 
   */
  protected[spi] def buildGraph(inputRDD: RDD[(Long, T)]): Graph[V, E]

  /*
   * Returns a graph given the vertex and edge RDDs
   */
  protected[spi] def buildGraph(vertexRDD: RDD[(Long, V)], edgeRDD: RDD[Edge[E]])(implicit tag: ClassTag[V], etag: ClassTag[E]): Graph[V, E] = {
    val graph = Graph(vertexRDD, edgeRDD)
    val result = graph.partitionBy(parameters.partitionStrategy)
    graph.edges.unpersist(false)

    result
  }

  /*
   * Validate using a fail fast policy
   */
  protected[spi] def validateNetwork() = parameters.metrics.foreach(validateMetric(_))

  private def validateMetric(metric: GraphMetric) = if ((metric.requiresDirected && !isDirected) || (metric.requiresWeighted && !isWeighted))
    throw new UnsupportedOperationException("Metric " + metric + " cannot be applied to the current network")

  /*
   * Compute metrics
   */
  protected[spi] def computeMetrics(graph: Graph[V, E])(implicit tag: ClassTag[V], etag: ClassTag[E]): Graph[V, E] = graph.computeMetrics(context, parameters, isDirected)

  /*
   * Apply filters
   */
  protected[spi] def applyFilters(graph: Graph[V, E])(implicit tag: ClassTag[V]): Graph[V, E] = graph.applyFilters(parameters)

  /*
   * is network directed
   */
  protected def isDirected: Boolean

  /*
   * is network weighted
   */
  protected def isWeighted: Boolean

  /*
   *  Default configuration values tuned to optimize the application 
   *  Kryo serialization is enforced with reference tracking enabled (pregel introduces cyclic references)
   *  
   *  Make sure the spark event log is disabled due to pregel (again). The size of events sent to the driver
   *  causes an outOfMemory exception when de-serializing the json stream
   *  
   */
  protected[spi] def getDefaultConf() = {
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      .set("spark.kryo.referenceTracking", "true")
      .set("spark.eventLog.enabled", "false")
      .set("spark.es.read.metadata", "true")

    GraphXKryo.registerKryoClasses(conf, defaultSerializableClasses ++ serializableClasses)
  }

  /*
   * Classes used in I/O operations 
   */
  private lazy val defaultSerializableClasses: Array[String] = Array("java.lang.Class", "scala.reflect.ClassTag$$anon$1",
    "[Lscala.Tuple3;", "[Lscala.Tuple2;", "org.apache.spark.graphx.impl.ShippableVertexPartition", "com.grafalgo.graph.spi.GraphVertex",
    "org.apache.spark.graphx.Edge", "org.apache.spark.graphx.Edge$mcJ$sp", "[Lorg.apache.spark.graphx.Edge;", "scala.collection.mutable.WrappedArray$ofRef",
    "scala.collection.immutable.$colon$colon", "scala.collection.immutable.Map$EmptyMap$", "scala.collection.immutable.Nil$",
    "scala.collection.immutable.Set$EmptySet$", "[Ljava.lang.String;", "[I", "org.apache.spark.graphx.impl.VertexAttributeBlock", "scala.None$", "[D",
    "org.apache.spark.mllib.stat.MultivariateOnlineSummarizer", "scala.math.Ordering$$anon$4", "scala.math.Ordering$$anon$11",
    "scala.math.Ordering$Long$", "com.grafalgo.graph.spi.ClusterProcessor$$anonfun$5", "scala.math.LowPriorityOrderingImplicits$$anon$6", "[Lcom.grafalgo.graph.spi.GraphVertex;",
    "scala.math.Ordering$Int$", "scala.math.Ordering$Double$", "scala.math.Ordering$$anon$5", "[Ljava.lang.Object;", "com.grafalgo.graph.spi.metric.Component$$anonfun$5",
    "com.grafalgo.graph.spi.GraphMetricsHolder", "[Lorg.apache.spark.util.collection.CompactBuffer;", "scala.reflect.ManifestFactory$$anon$1", "java.lang.Object",
    "scala.collection.mutable.Map",  "[Lscala.collection.mutable.Map;", "scala.collection.immutable.List", "[Lcom.grafalgo.graph.spi.GraphEdge;",
    "com.grafalgo.graph.spi.GraphEdge","scala.reflect.ManifestFactory$$anon$4","spire.CompatPriority1$$anon$4","spire.math.IntIsNumeric",
    "com.grafalgo.graph.spi.ScalarProcessor$$anonfun$getTop$1","com.grafalgo.graph.spi.GraphMetrics$$anon$12$$anon$1","com.grafalgo.graph.spi.ScalarOperations$$anon$2",
    "spire.math.DoubleIsNumeric","com.grafalgo.graph.spi.ScalarOperations$$anon$3", "com.grafalgo.graph.spi.GraphMetrics$$anon$13$$anon$2",
    "com.grafalgo.graph.spi.GraphMetrics$$anon$14$$anon$3","com.grafalgo.graph.spi.GraphMetrics$$anon$15$$anon$4",
    "com.grafalgo.graph.spi.GraphMetrics$$anon$16$$anon$5","com.grafalgo.graph.spi.GraphMetrics$$anon$17$$anon$6","com.grafalgo.graph.spi.GraphMetrics$$anon$18$$anon$7",
    "com.grafalgo.graph.spi.GraphMetrics$$anon$19$$anon$8","com.grafalgo.graph.spi.GraphMetrics$$anon$20$$anon$9","com.grafalgo.graph.spi.GraphMetrics$$anon$21$$anon$10",
    "com.grafalgo.graph.spi.GraphMetrics$$anon$22$$anon$11") ++
    //Pregel serialization dependencies
    Array("org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap$mcJI$sp",
      "scala.reflect.ManifestFactory$$anon$9", "scala.reflect.ManifestFactory$$anon$10", "org.apache.spark.util.collection.OpenHashSet$LongHasher",
      "org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap$$anonfun$1",
      "org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap$$anonfun$2",
      "org.apache.spark.graphx.impl.RoutingTablePartition") ++
      //Hadoop
      Array("org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text") ++
      //spi metric
      Array("[Lcom.grafalgo.graph.spi.metric.ModularityGephi$CommunityStructure;","com.grafalgo.graph.spi.metric.ModularityGephi$CommunityStructure",
          "com.grafalgo.graph.spi.metric.ModularitySotera$LouvainData", "[Lcom.grafalgo.graph.spi.metric.ModularitySotera$LouvainData;") ++
      Array("com.grafalgo.graph.spi.metric.HBSEConf", "com.grafalgo.graph.spi.metric.PartialDependency", "com.grafalgo.graph.spi.metric.PathData",
          "com.grafalgo.graph.spi.metric.ShortestPathList", "[Lcom.grafalgo.graph.spi.metric.HBSEData;","com.grafalgo.graph.spi.metric.HBSEData", "com.grafalgo.graph.spi.metric.HighBetweennessCore",
          "org.apache.spark.util.BoundedPriorityQueue", "com.grafalgo.graph.spi.metric.HighBetweennessCore$$anon$1")    

  /*
   * Classes used in I/O operations declared by the extending jobs
   */
  protected val serializableClasses: Array[String]

  /*
   * Job name 
   */
  protected val jobName: String

  /*
   * Close operation must be handled with care, There can be only one spark context object per class loader and, 
   * although it is thread safe, existing RDD abstractions belonging to threads sharing the context will become
   * not accessible and further operations will fail. 
   */
  override def close = { if (sc != null) { this.synchronized { if (sc != null) { sc.stop; sc = null } } } }

  /*
   * Get the source property
   */
  protected def getSource = jobName + parameters.source

  /*
   * Get the exporter property
   */
  protected def getExporter = jobName + parameters.exporter
}

/*
 * Companion Factory Provider
 */
object NetworkJob extends ServiceProvider[NetworkJob[_, _ <: GraphVertex, _ <: GraphEdge]] {
  override lazy val fileName = "job-service"
  override lazy val serviceRoot = "job"

  override def getDefaultServiceName: String = null

}

//Main entry point
object NetworkJobLauncher extends LazyLogging {
  def main(args: Array[String]) {
    for (job <- NetworkJob[NetworkJob[_, _ <: GraphVertex, _ <: GraphEdge]](NetworkParameters.network)) {
      job.buildNetwork
    }
  }
}

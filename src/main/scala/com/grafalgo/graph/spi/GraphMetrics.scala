package com.grafalgo.graph.spi

import scala.collection.Set
import scala.reflect.ClassTag
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import ScalarOperations.DoubleOps
import ScalarOperations.IntOps
import ScalarOperations.LongOps
import com.grafalgo.graph.spi.metric.Centrality
import com.grafalgo.graph.spi.metric.Component
import com.grafalgo.graph.spi.metric.Degree
import com.grafalgo.graph.spi.metric.ModularityGephi
import com.grafalgo.graph.spi.metric.ModularitySotera
import com.grafalgo.graph.spi.util.scala.LazyLogging
import spire.math.Numeric
import com.grafalgo.graph.spi.metric.Betweenness
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

/**
 * Graph metrics definition
 *
 * @author guregodevo
 */
object GraphMetrics extends Enumeration with LazyLogging {

  type GraphMetric = Value
  //Metrics
  protected abstract class Metric[S: Numeric: ScalarOperations](val requiresDirected: Boolean, val requiresWeighted: Boolean, clusterMetric: Boolean = false) extends Val {

    private lazy val processor = getProcessor()

    /*
    * Returns the processor with the specified filter thresholds
    */
    def getFilter(min: String, max: String): ScalarMetricProcessor[S] =
      if (!clusterMetric) getProcessor(min, max).asInstanceOf[ScalarMetricProcessor[S]] else
        throw new UnsupportedOperationException("Cannot filter by a cluster id range")
    /*
    * Returns the graph with the computed requested metric 
    */
    def computeMetric[V <: GraphVertex, E <: GraphEdge](sc: SparkContext, holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E], parameters: NetworkParameters, isDirected: Boolean)(implicit tag: ClassTag[V], etag: ClassTag[E]): RDD[Tuple2[VertexId, GraphMetricsHolder]] =
      throw new UnsupportedOperationException("Metric " + toString + " does not exist")
    /*
    * Returns the group of metrics associated to this one. Defaults to the metric itself
    */
    def getAssociatedMetrics: Set[GraphMetric] = Set(this)
    /*
    * Returns the associated value processor
    */
    def getMetricProcessor: MetricProcessor[S, GraphVertex] = processor

    protected def getProcessor(min: String = null, max: String = null): MetricProcessor[S, GraphVertex] = throw new UnsupportedOperationException("Metric values are accessed through associated slots")
  }

  final implicit def toMetric(x: Value) = x.asInstanceOf[Metric[_]]

  final val DEGREE = new Metric[Int](false, false) {
    override def computeMetric[V <: GraphVertex, E <: GraphEdge](sc: SparkContext, holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E], parameters: NetworkParameters, isDirected: Boolean)(implicit tag: ClassTag[V], etag: ClassTag[E]) =
      Degree.computeDegreeMetric(holderRDD, graph)

    override def getProcessor(min: String = null, max: String = null) = new ScalarMetricProcessor[Int](toString, min, max) {
      override def get(node: GraphVertex) = node.degree
      override def put(value: Int, node: GraphVertex): Unit = node.degree = value
    }
  }
  final val DEGREES = new Metric[Int](true, false) {
    override def computeMetric[V <: GraphVertex, E <: GraphEdge](sc: SparkContext, holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E], parameters: NetworkParameters, isDirected: Boolean)(implicit tag: ClassTag[V], etag: ClassTag[E]) =
      Degree.computeDegreesMetric(holderRDD, graph)
    override def getAssociatedMetrics = Set(DEGREE, INDEGREE, OUTDEGREE)

  }
  final val WEIGHTEDDEGREE = new Metric[Int](false, true) {
    override def computeMetric[V <: GraphVertex, E <: GraphEdge](sc: SparkContext, holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E], parameters: NetworkParameters, isDirected: Boolean)(implicit tag: ClassTag[V], etag: ClassTag[E]) =
      Degree.computeWeightedDegreeMetric(holderRDD, graph)
    override def getAssociatedMetrics = Set(DEGREE, this)
    override def getProcessor(min: String = null, max: String = null) = new ScalarMetricProcessor[Int](toString, min, max) {
      override def get(node: GraphVertex) = node.weightedDegree
      override def put(value: Int, node: GraphVertex): Unit = node.weightedDegree = value
    }
  }
  final val WEIGHTEDDEGREES = new Metric[Int](true, true) {
    override def computeMetric[V <: GraphVertex, E <: GraphEdge](sc: SparkContext, holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E], parameters: NetworkParameters, isDirected: Boolean)(implicit tag: ClassTag[V], etag: ClassTag[E]) =
      Degree.computeWeightedDegreesMetric(holderRDD, graph)
    override def getAssociatedMetrics = Set(DEGREE, INDEGREE, OUTDEGREE, WEIGHTEDDEGREE, WEIGHTEDINDEGREE, WEIGHTEDOUTDEGREE)
  }
  final val INDEGREE = new Metric[Int](true, false) {
    override def getProcessor(min: String = null, max: String = null) = new ScalarMetricProcessor[Int](toString, min, max) {
      override def get(node: GraphVertex) = node.inDegree
      override def put(value: Int, node: GraphVertex): Unit = node.inDegree = value
    }
  }
  final val OUTDEGREE = new Metric[Int](true, false) {
    override def getProcessor(min: String = null, max: String = null) = new ScalarMetricProcessor[Int](toString, min, max) {
      override def get(node: GraphVertex) = node.outDegree
      override def put(value: Int, node: GraphVertex): Unit = node.outDegree = value
    }
  }
  final val WEIGHTEDINDEGREE = new Metric[Int](true, true) {
    override def getProcessor(min: String = null, max: String = null) = new ScalarMetricProcessor[Int](toString, min, max) {
      override def get(node: GraphVertex) = node.weightedInDegree
      override def put(value: Int, node: GraphVertex): Unit = node.weightedInDegree = value
    }
  }
  final val WEIGHTEDOUTDEGREE = new Metric[Int](true, true) {
    override def getProcessor(min: String = null, max: String = null) = new ScalarMetricProcessor[Int](toString, min, max) {
      override def get(node: GraphVertex) = node.weightedOutDegree
      override def put(value: Int, node: GraphVertex): Unit = node.weightedOutDegree = value
    }
  }
  final val EIGENCENTRALITY = new Metric[Double](false, false) {
    override def computeMetric[V <: GraphVertex, E <: GraphEdge](sc: SparkContext, holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E], parameters: NetworkParameters, isDirected: Boolean)(implicit tag: ClassTag[V], etag: ClassTag[E]) =
      Centrality.computeEigenVectorCentralityMetric(holderRDD, graph, parameters.eigenCentralityIterations, isDirected)

    override def getProcessor(min: String = null, max: String = null) = new ScalarMetricProcessor[Double](toString, min, max) {
      override def get(node: GraphVertex) = node.eigenCentrality
      override def put(value: Double, node: GraphVertex): Unit = node.eigenCentrality = value
    }

  }
  final val BETWEENNESSCENTRALITY = new Metric[Double](false, false) {
    override def computeMetric[V <: GraphVertex, E <: GraphEdge](sc: SparkContext, holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E], parameters: NetworkParameters, isDirected: Boolean)(implicit tag: ClassTag[V], etag: ClassTag[E]) =
      Betweenness.computeCentralityMetric(sc, holderRDD, graph, parameters)

    override def getProcessor(min: String = null, max: String = null) = new ScalarMetricProcessor[Double](toString, min, max) {
      override def get(node: GraphVertex) = node.betweennessCentrality
      override def put(value: Double, node: GraphVertex): Unit = node.betweennessCentrality = value
    }

  }
  final val PAGERANK = new Metric[Double](true, false) {
    override def computeMetric[V <: GraphVertex, E <: GraphEdge](sc: SparkContext, holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E], parameters: NetworkParameters, isDirected: Boolean)(implicit tag: ClassTag[V], etag: ClassTag[E]) =
      Centrality.computePageRankMetric(holderRDD, graph, parameters.pageRankResetProb, parameters.pageRankTolerance, parameters.pageRankIterations, isDirected)

    override def getProcessor(min: String = null, max: String = null) = new ScalarMetricProcessor[Double](toString, min, max) {
      override def get(node: GraphVertex) = node.pageRank
      override def put(value: Double, node: GraphVertex): Unit = node.pageRank = value
    }
  }
  final val MODULARITY = new Metric[Long](false, false, true) {
    override def computeMetric[V <: GraphVertex, E <: GraphEdge](sc: SparkContext, holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E], parameters: NetworkParameters, isDirected: Boolean)(implicit tag: ClassTag[V], etag: ClassTag[E]) =
        ModularityGephi.computeMetric(holderRDD, graph, parameters)

    override def getProcessor(min: String = null, max: String = null) = new ClusterMetricProcessor[Long](toString) {
      override def get(node: GraphVertex) = node.modularity
      override def put(value: Long, node: GraphVertex): Unit = node.modularity = value
    }

  }
  final val COMPONENT = new Metric[Long](false, false, true) {
    override def computeMetric[V <: GraphVertex, E <: GraphEdge](sc: SparkContext, holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E], parameters: NetworkParameters, isDirected: Boolean)(implicit tag: ClassTag[V], etag: ClassTag[E]) =
      Component.computeMetric(holderRDD, graph)

    override def getProcessor(min: String = null, max: String = null) = new ClusterMetricProcessor[Long](toString) {
      override def get(node: GraphVertex) = node.component
      override def put(value: Long, node: GraphVertex): Unit = node.component = value
    }
  }

  /*
   * Class indexing all the requested + already present metrics.
   */
  class MetricSlots(val slots: Array[GraphMetric], val requestDirected: Boolean) {
    def getProcessors = { var index = -1; for (slot <- slots) yield { index += 1; slot.getMetricProcessor } }
  }

  /*
   * Trait with the set of operations decorating the graph.
   */
  trait Metrics[V <: GraphVertex, E <: GraphEdge] {

    protected val graph: Graph[V, E]

    /*
     * distributes the graph using the desired strategy and aggregates the edges
     * 
     * TODO The decorator is metric-related, this needs some re-factoring move this as a general graph
     * operations decorator
     */
    def distributeGraph(partitionStrategy: PartitionStrategy, f: (E, E) => E): Graph[V, E] = {

      val partition = graph.partitionBy(partitionStrategy);
      val result = partition.groupEdges(f)
      result.cache
      result.edges.foreach { x => None }

      graph.edges.unpersist(false)
      partition.edges.unpersist(false)

      logger.info("Graph partitions number: {}", result.edges.partitions.size.toString)
      result
    }

    /*
     * Compute the corresponding metric 
     */
    def computeMetric(sc: SparkContext, holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], metric: GraphMetric, parameters: NetworkParameters, isDirected: Boolean)(implicit tag: ClassTag[V], eTag: ClassTag[E]): RDD[Tuple2[VertexId, GraphMetricsHolder]] = {
      logger.info("Computing metric: " + metric.toString)

      val start = System.nanoTime;
      val result = metric.computeMetric(sc, holderRDD, this.graph, parameters, isDirected)
      result.persist(StorageLevel.MEMORY_AND_DISK)
      result.foreachPartition(x => None)

      logger.info("Time consumed (ms): " + (System.nanoTime() - start) / 1000000)

      result
    }

    /*
     *  Compute the corresponding metrics in sequence
     */
    def computeMetrics(sc: SparkContext, parameters: NetworkParameters, isDirected: Boolean)(implicit tag: ClassTag[V], eTag: ClassTag[E]): Graph[V, E] = {

      if (parameters.metrics.size > 0) {
        logger.info("Computing Graph metrics")
        var holderRDD = graph.vertices.map({ case (id, _) => (id, new GraphMetricsHolder()) })
        holderRDD.persist(StorageLevel.MEMORY_AND_DISK)
        holderRDD.foreachPartition(x => None)
        parameters.metrics.foreach { metric => var prevHolderRDD = holderRDD; holderRDD = computeMetric(sc, prevHolderRDD, metric, parameters, isDirected); prevHolderRDD.unpersist(false) }
        val metrics = parameters.requestedMetrics
        graph.joinVertices(holderRDD)({ case (vid, v, h) => v.withMetrics(h, metrics) })
      }
      else
        graph
    }

    /*
     * Applies the filter and returns the filtered graph
     */
    def applyFilter(filter: ScalarMetricProcessor[_])(implicit tag: ClassTag[V]) = graph.subgraph(vpred = (id, x) => filter.filter(x))

    /*
     * Applies the sequence of filters and returns the filtered graph
     */
    def applyFilters(parameters: NetworkParameters)(implicit tag: ClassTag[V]) = {
      var result: Graph[V, E] = graph
      val metricNames = for (metric <- GraphMetrics.values) yield (metric.toString)
      parameters.filters.foreach { filter =>
        {

          if (metricNames.contains(filter.name)) {
            var prev = result
            result = prev.applyFilter(filter)
            prev.unpersist(false)
            prev.edges.unpersist(false)
          }
          else
            logger.warn("Cannot filter by metric " + filter.name + ". It is not present")
        }
      }

      result
    }

    /**
     * Returns either the initial graph or the extracted giant component depending on the flag value
     */
    def extractGiantComponent(parameters: NetworkParameters)(implicit tag: ClassTag[V], eTag: ClassTag[E]): Graph[V, E] =
      if (parameters.extractGiant) Component.extractGiantComponent(graph) else graph
  }

  /** Implicit Decorator */

  implicit class MetricDecorator[V <: GraphVertex, E <: GraphEdge](override val graph: Graph[V, E]) extends Metrics[V, E]

}

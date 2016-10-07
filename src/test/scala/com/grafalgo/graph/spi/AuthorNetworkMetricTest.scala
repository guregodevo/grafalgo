package com.grafalgo.graph.spi

import org.junit.Assert._
import org.junit.Before
import org.junit.Test

import com.grafalgo.graph.spi.GraphMetrics;
import com.grafalgo.graph.spi.NetworkParameters;
import com.grafalgo.graph.spi.GraphMetrics._;
import com.grafalgo.graph.spi.NetworkParameters._;
import com.typesafe.config.ConfigFactory
import com.grafalgo.graph.elastic.ElasticIntegrationTest
import com.grafalgo.graph.elastic.ElasticIntegrationTest._
import com.grafalgo.graph.network.author.ItemAuthorData
import com.grafalgo.graph.network.author.AuthorEdge
import com.grafalgo.graph.network.author.AuthorVertex
import com.grafalgo.graph.spi.GraphMetrics._
import com.grafalgo.graph.spi.util.scala.AutoResource.Arm
import org.junit.Ignore
import com.grafalgo.graph.network.author.AuthorNetwork
import com.grafalgo.graph.spi.metric.Component
import com.grafalgo.graph.spi._

/**
 * NickNetwork metrics tests
 *
 * @author guregodevo
 */

class AuthorNetworkMetricTest extends ElasticIntegrationTest {

  private val cs = CONNECTION
  private val outputPath = "gexf/"

  private type JobType = NetworkJob[ItemAuthorData, AuthorVertex, AuthorEdge]

  @Before
  def setUpContext {
    System.setProperty("config.file", "./src/test/resources/authornetwork.conf")
    ConfigFactory.invalidateCaches
    System.setProperty("spark.master", "local[2]")
    System.setProperty("spark.appName", "AuthorNetworkMetricTest")
    try {
      deleteIndex
    }
    catch { case _: Exception => None }
    createIndex
    populateSampleItemData

  }

  @Test
  def testCentrality {
    var parameters = new NetworkParameters() { override def getMetrics = Array[GraphMetric](GraphMetrics.PAGERANK) }
    for (job <- new AuthorNetwork(parameters)) {
      val (graph, metrics) = job.buildTheGraph
      val results = graph.computeMetrics(job.context, parameters, job.isDirected).vertices.collect.toMap

      assertEquals(1.1831366975198625, results(1).pageRank, 0)
      assertEquals(1.527782902438515, results(2).pageRank, 0)
      assertEquals(0.9268077335363691, results(3).pageRank, 0)
      assertEquals(0.15, results(4).pageRank, 0)

    }

    parameters = new NetworkParameters() { override def getMetrics = Array[GraphMetric](GraphMetrics.EIGENCENTRALITY) }
    for (job <- new AuthorNetwork(parameters)) {
      val (graph, metrics) = job.buildTheGraph
      val results = graph.computeMetrics(job.context, parameters, job.isDirected).vertices.collect.toMap

      assertEquals(1.0, results(1).eigenCentrality, 0)
      assertEquals(1.0, results(2).eigenCentrality, 0)
      assertEquals(0.6187484941488304, results(3).eigenCentrality, 0)
      assertEquals(0.0, results(4).eigenCentrality, 0)
    }

  }

  @Test
  def testNetworkDegree {

    val parameters = new NetworkParameters() { override def getMetrics = Array[GraphMetric](GraphMetrics.DEGREE) }
    for (job <- new AuthorNetwork(parameters)) {
      val (graph, metrics) = job.buildTheGraph
      val results = graph.computeMetrics(job.context, parameters, job.isDirected).vertices.collect.toMap

      assertEquals(3, results(1).degree)
      assertEquals(4, results(2).degree)
      assertEquals(4, results(3).degree)
      assertEquals(1, results(4).degree)
    }
  }

  @Test
  def testNetworkInDegree {

    val parameters = new NetworkParameters() { override def getMetrics = Array[GraphMetric](GraphMetrics.DEGREES) }
    for (job <- new AuthorNetwork(parameters)) {
      val (graph, metrics) = job.buildTheGraph
      val results = graph.computeMetrics(job.context, parameters, job.isDirected).vertices.collect.toMap

      assertEquals(2, results(1).inDegree)
      assertEquals(2, results(2).inDegree)
      assertEquals(2, results(3).inDegree)
      assertEquals(0, results(4).inDegree)
    }
  }

  @Test
  def testNetworkOutDegree {

    val parameters = new NetworkParameters() { override def getMetrics = Array[GraphMetric](GraphMetrics.DEGREES) }
    ConfigFactory.invalidateCaches
    for (job <- new AuthorNetwork(parameters)) {
      val (graph, metrics) = job.buildTheGraph
      val results = graph.computeMetrics(job.context, parameters, job.isDirected).vertices.collect.toMap

      assertEquals(1, results(1).outDegree)
      assertEquals(2, results(2).outDegree)
      assertEquals(2, results(3).outDegree)
      assertEquals(1, results(4).outDegree)
    }
  }

  @Test
  def testNetworkWeightedDegree {
    val parameters = new NetworkParameters() { override def getMetrics = Array[GraphMetric](GraphMetrics.WEIGHTEDDEGREE) }
    for (job <- new AuthorNetwork(parameters)) {
      val (graph, metrics) = job.buildTheGraph
      val results = graph.computeMetrics(job.context, parameters, job.isDirected).vertices.collect.toMap

      assertEquals(4, results(1).weightedDegree)
      assertEquals(5, results(2).weightedDegree)
      assertEquals(4, results(3).weightedDegree)
      assertEquals(1, results(4).weightedDegree)
    }
  }

  @Test
  def testNetworkWeightedInDegree {

    val parameters = new NetworkParameters() { override def getMetrics = Array[GraphMetric](GraphMetrics.WEIGHTEDDEGREES) }
    for (job <- new AuthorNetwork(parameters)) {
      val (graph, metrics) = job.buildTheGraph
      val results = graph.computeMetrics(job.context, parameters, job.isDirected).vertices.collect.toMap

      assertEquals(3, results(1).weightedInDegree)
      assertEquals(2, results(2).weightedInDegree)
      assertEquals(2, results(3).weightedInDegree)
      assertEquals(0, results(4).weightedInDegree)
    }
  }

  @Test
  def testNetworkWeightedOutDegree {

    val parameters = new NetworkParameters() { override def getMetrics = Array[GraphMetric](GraphMetrics.WEIGHTEDDEGREES) }
    for (job <- new AuthorNetwork(parameters)) {
      val (graph, metrics) = job.buildTheGraph
      val results = graph.computeMetrics(job.context, parameters, job.isDirected).vertices.collect.toMap

      assertEquals(1, results(1).weightedOutDegree)
      assertEquals(3, results(2).weightedOutDegree)
      assertEquals(2, results(3).weightedOutDegree)
      assertEquals(1, results(4).weightedOutDegree)
    }
  }

  @Test
  def testNetworkDynamicMetric {

    val parameters = new NetworkParameters() { override def getMetrics = Array[GraphMetric](GraphMetrics.WEIGHTEDDEGREES) }
    for (job <- new AuthorNetwork(parameters)) {
      val (graph, metrics) = job.buildTheGraph
      val results = graph.computeMetrics(job.context, parameters, job.isDirected).vertices.collect.toMap

      assertEquals(1, results(1).weightedOutDegree)
      assertEquals(3, results(2).weightedOutDegree)
      assertEquals(2, results(3).weightedOutDegree)
      assertEquals(1, results(4).weightedOutDegree)
    }
  }

  @Test
  def testNetworkDynamicMetrics {

    val parameters = new NetworkParameters() { override def getMetrics = Array[GraphMetric](GraphMetrics.WEIGHTEDDEGREE, GraphMetrics.DEGREE) }
    for (job <- new AuthorNetwork(parameters)) {
      val (graph, metrics) = job.buildTheGraph
      val results = graph.computeMetrics(job.context, parameters, job.isDirected).vertices.collect.toMap

      assertEquals(3, results(1).degree)
      assertEquals(4, results(2).degree)
      assertEquals(4, results(3).degree)
      assertEquals(1, results(4).degree)

      assertEquals(4, results(1).weightedDegree)
      assertEquals(5, results(2).weightedDegree)
      assertEquals(4, results(3).weightedDegree)
      assertEquals(1, results(4).weightedDegree)

      val output = graph.vertices.sortByKey().collect

      for (j <- 0 to 2) {
        assertEquals("author" + (j + 1), output(j)._2.nickName)
      }

      val author1 = output(0)._2
      val author2 = output(1)._2

      assertEquals(3, author1.urlNumber)
      assertEquals(2, author1.domainNumber)
      assertEquals(2, author2.urlNumber)
      assertEquals(1, author2.domainNumber)
    }
  }

  @Test
  def testNetworkIncrementalDynamicMetric {

    val parameters = new NetworkParameters() { override def getMetrics = Array[GraphMetric](GraphMetrics.COMPONENT, GraphMetrics.WEIGHTEDDEGREES) }

    for (job <- new AuthorNetwork(parameters)) {

      val (g, metrics) = job.buildTheGraph
      val graph = g.computeMetrics(job.context, parameters, job.isDirected)

      var results = graph.vertices.collect.toMap

      assertEquals(3, results(1).degree)
      assertEquals(4, results(2).degree)
      assertEquals(4, results(3).degree)
      assertEquals(1, results(4).degree)

      //      degreeMetric.edges.foreach { x => println(x.srcId +"--("+x.attr.weight +")->"+x.dstId) }
      assertEquals(4, results(1).weightedDegree)
      assertEquals(5, results(2).weightedDegree)
      assertEquals(4, results(3).weightedDegree)
      assertEquals(1, results(4).weightedDegree)

      assertEquals(1l, results(1).component)
      assertEquals(1l, results(2).component)
      assertEquals(1l, results(3).component)
      assertEquals(1l, results(4).component)
    }
  }

  @Test
  def testModularityMetric {
    val parameters = new NetworkParameters() { override def getMetrics = Array[GraphMetric](GraphMetrics.MODULARITY) }

    for (job <- new AuthorNetwork(parameters)) {

      val (graph, metrics) = job.buildTheGraph

      val results = graph.computeMetrics(job.context, parameters, job.isDirected).vertices.collect.toMap

      println(results.map({ case (id, v) => (id, v.modularity) }))
      /*assertEquals(1, accessor.get(results(1)))
      assertEquals(2, accessor.get(results(2)))
      assertEquals(3, accessor.get(results(3)))
      assertEquals(4, accessor.get(results(4)))*/

    }
  }

  @Test
  def testConnectedComponents {

    val parameters = new NetworkParameters() { override def getMetrics = Array[GraphMetric](GraphMetrics.COMPONENT) }
    for (job <- new AuthorNetwork(parameters)) {
      val (graph, metrics) = job.buildTheGraph
      val results = graph.computeMetrics(job.context, parameters, job.isDirected).vertices.collect.toMap

      //results.foreach(x => println(x._2.nickName + " " +x._2.component))
      assertEquals(1l, results(1).component)
      assertEquals(1l, results(2).component)
      assertEquals(1l, results(3).component)
      assertEquals(1l, results(4).component)
    }

    for (job <- new AuthorNetwork(new NetworkParameters())) {
      val results = Component.extractGiantComponent(job.buildTheGraph._1).vertices.collect.toMap
      assertEquals(4, results.size)
    }
  }

}

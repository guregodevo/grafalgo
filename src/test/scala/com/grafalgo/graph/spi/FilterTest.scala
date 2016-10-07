package com.grafalgo.graph.spi

import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Ignore
import org.junit.Test

import com.grafalgo.graph.spi.GraphMetrics;
import com.grafalgo.graph.spi.NetworkParameters;
import com.grafalgo.graph.spi.GraphMetrics._;
import com.typesafe.config.ConfigFactory
import com.grafalgo.graph.elastic.ElasticIntegrationTest
import com.grafalgo.graph.elastic.ElasticIntegrationTest._
import com.grafalgo.graph.network.author.ItemAuthorData
import com.grafalgo.graph.network.author.AuthorEdge
import com.grafalgo.graph.network.author.AuthorNetwork
import com.grafalgo.graph.network.author.AuthorVertex
import com.grafalgo.graph.spi.util.scala.AutoResource.Arm
import com.grafalgo.graph.spi.GraphMetrics._
import com.grafalgo.graph.spi._

class FilterTest extends ElasticIntegrationTest {
  private val cs = CONNECTION
  private val outputPath = "gexf/"

  private type JobType = NetworkJob[ItemAuthorData, AuthorVertex, AuthorEdge]

  @Before
  def setUpContext {
    System.setProperty("config.file", "./src/test/resources/authornetwork.conf")
    ConfigFactory.invalidateCaches
    System.setProperty("spark.master", "local[2]")
    System.setProperty("spark.appName", "FilterTest")
    try {
      deleteIndex
    }
    catch { case _: Exception => None }
    createIndex
    populateSampleItemData
  }

  @Test
  def testFilter {
    val parameters = new NetworkParameters { override def getMetrics = Array[GraphMetric](GraphMetrics.WEIGHTEDDEGREES) }

    for (job <- new AuthorNetwork(parameters)) {

      val graph = job.buildGraph(job.buildInputRDD).computeMetrics(job.context, parameters, job.isDirected)
      val results = graph.vertices.collect.toMap
      
      assertEquals(2, results(1).inDegree)
      assertEquals(2, results(2).inDegree)
      assertEquals(2, results(3).inDegree)
      assertEquals(0, results(4).inDegree)
  

      var filteredGraph = graph.applyFilter(GraphMetrics.withName(INDEGREE.toString).getFilter("1", "3"))

      var filteredResults = filteredGraph.vertices.collect.toMap
      assertEquals(2, results(1).inDegree)
      assertEquals(2, results(2).inDegree)
      assertEquals(2, results(3).inDegree)
      assertEquals(3, filteredResults.keys.size)

      filteredGraph = graph.applyFilter(GraphMetrics.withName(INDEGREE.toString).getFilter("-1", "1"))

      filteredResults = filteredGraph.vertices.collect.toMap
      assertEquals(0, results(4).inDegree)
      assertEquals(1, filteredResults.keys.size)

    }
  }

  @Test
  def testValidConfiguration {
    System.setProperty("spark.master", "local[2]")
    System.setProperty("spark.appName", "NickNetworkJobTest")
    System.setProperty("config.file", "./src/test/resources/filterWithMetric.conf")
    ConfigFactory.invalidateCaches
    assertEquals(GraphMetrics.INDEGREE.toString, new NetworkParameters {}.filters(0).name)
  }

  @Ignore
  @Test(expected = classOf[IllegalStateException])
  def testInvalidConfiguration {
    System.setProperty("spark.master", "local[2]")
    System.setProperty("spark.appName", "NickNetworkJobTest")
    System.setProperty("config.file", "./src/test/resources/filterWithoutMetric.conf")
    ConfigFactory.invalidateCaches
    new NetworkParameters {}.filters
  }
}
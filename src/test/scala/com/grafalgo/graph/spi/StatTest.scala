package com.grafalgo.graph.spi

import com.grafalgo.graph.network.author.AuthorVertex
import org.junit.Before
import com.grafalgo.graph.network.author.AuthorEdge

import com.grafalgo.graph.spi.GraphMetrics;
import com.grafalgo.graph.spi.NetworkParameters;
import com.grafalgo.graph.spi.ScalarMetricProcessor;
import com.grafalgo.graph.spi.GraphMetrics._;
import com.typesafe.config.ConfigFactory
import com.grafalgo.graph.network.author.ItemAuthorData
import com.grafalgo.graph.elastic.ElasticIntegrationTest._
import com.grafalgo.graph.elastic.ElasticIntegrationTest
import com.grafalgo.graph.spi.util.scala.AutoResource._
import org.junit.Test
import org.junit.Assert._
import com.grafalgo.graph.spi.GraphMetrics._
import com.grafalgo.graph.network.author.AuthorVertex
import com.grafalgo.graph.network.author.AuthorNetwork
import com.grafalgo.graph.spi._

class StatTest extends ElasticIntegrationTest {
  private val cs = CONNECTION
  private val outputPath = "gexf/"

  private type JobType = NetworkJob[ItemAuthorData, AuthorVertex, AuthorEdge]

  @Before
  def setUpContext {
    System.setProperty("config.file", "./src/test/resources/authornetwork.conf")
    ConfigFactory.invalidateCaches
    System.setProperty("spark.master", "local[2]")
    System.setProperty("spark.appName", "NickNetworkJobTest")
    try {
      deleteIndex
    }
    catch { case _: Exception => None }
    createIndex
    populateSampleItemData
  }

  @Test
  def testQuantiles {
    
    val parameters = new NetworkParameters { override def getMetrics = Array[GraphMetric](GraphMetrics.WEIGHTEDDEGREES) }
    val slots = new MetricSlots(parameters.requestedMetrics, true)
    for (job <- new AuthorNetwork(parameters)) {
      val graph = job.buildGraph(job.buildInputRDD).computeMetrics(job.context, parameters, job.isDirected)
      
      val p = graph.vertices.map(_._2)     
      
      val quantiles = GraphMetrics.DEGREE.getMetricProcessor.asInstanceOf[ScalarMetricProcessor[_]].getQuantiles(p, 4)._1
      assertEquals(1, quantiles(0))
      assertEquals(3, quantiles(1))
      assertEquals(4, quantiles(2))
      assertEquals(4, quantiles(3))
    }
  }

}
package com.grafalgo.graph.spi

import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.junit.Before
import org.junit.Test
import org.junit.Assert._

import com.grafalgo.graph.spi.NetworkParameters._
import com.grafalgo.graph.spi.GraphMetrics._;
import com.typesafe.config.ConfigFactory
import com.grafalgo.graph.elastic.ElasticIntegrationTest
import com.grafalgo.graph.elastic.ElasticIntegrationTest._
import com.grafalgo.graph.network.ego.AuthorEdge
import com.grafalgo.graph.network.ego.AuthorVertex
import com.grafalgo.graph.network.ego.EgoNetwork
import com.grafalgo.graph.network.ego.ItemEgoData
import com.grafalgo.graph.source.ElasticDataSource
import com.grafalgo.graph.spi.util.scala.AutoResource.Arm
import com.grafalgo.graph.spi._

import java.io.File

class EgoNetworkTest extends ElasticIntegrationTest {
  private val cs = CONNECTION
  private val outputPath = "gexf/"
  private val SAMPLE_EDGES = List((3, 1, 1), (4, 1, 3), (2, 1, 3), (3, 1, 2))
  private var parameters: NetworkParameters = null

  private type JobType = NetworkJob[ItemEgoData, AuthorVertex, AuthorEdge]

  @Before
  def setUpContext {

    ConfigFactory.invalidateCaches
    System.setProperty("config.file", "./src/test/resources/egonetwork.conf")
    System.setProperty("spark.master", "local[2]")
    System.setProperty("spark.appName", "EgoNetworkTest")
    parameters = new NetworkParameters() {
      override val connectionString = DEFAULT_INDEX_TYPE + ElasticDataSource.CONNECTION_SEPARATOR + "{ \"query\":" +
        "{\"bool\":" +
        "{\"filter\":" +
        "{\"bool\":" +
        "{ \"should\":" +
        "[{\"term\": {\"nick_id\": \"4\"}},{\"term\": {\"item_parentNickId\": \"4\"}}]" +
        "}}}}}"
    }
    try {
      deleteIndex
    }
    catch { case _: Exception => None }
    createIndex
    populateSampleItemData
  }

  @Test
  def testEgoNetworkInputRDD {

    for (job <- NetworkJob[JobType]("EgoNetwork", parameters)) {

      val inputRDD = job.buildInputRDD.cache

      val output = inputRDD.sortByKey().collect

      assertEquals(4, inputRDD.count)

      assertEquals(4L, output(0)._1)
      assertEquals(6L, output(1)._1)
      assertEquals(8L, output(2)._1)
      assertEquals(9L, output(3)._1)
    }

  }

  @Test
  def testEgoNetworkVertexRDD {

    for (job <- NetworkJob[JobType]("EgoNetwork", parameters)) {

      val vertexRDD = job.buildGraph(job.buildInputRDD).vertices.cache

      assertEquals(4, vertexRDD.count)

      val output = vertexRDD.sortByKey().collect

      for (j <- 0 to 2) {
        assertEquals("author" + (j + 1), output(j)._2.nickName)
      }

      val author1 = output(0)._2
      val author4 = output(3)._2

      assertEquals(getDate("01-06-2015"), author1.firstItemDate)
      assertEquals(getDate("01-06-2015"), author1.lastItemDate)
      assertEquals(2, author4.urlNumber)
      assertEquals(2, author4.domainNumber)

    }
  }

  @Test
  def testEgoNetworkEdgeRDD {

    for (job <- NetworkJob[JobType]("EgoNetwork", parameters)) {
      val result = job.buildGraph(job.buildInputRDD).edges.collect
      //job.buildGraph(job.buildInputRDD).edges.foreach { x => println(x.srcId +"--("+x.attr.weight +")->"+x.dstId) }
      assertEquals(4, result.size)
      val triplets = result.map(x => (x.srcId, x.attr.weight, x.dstId))
      SAMPLE_EDGES.foreach(x => assertTrue(triplets.contains(x)))
    }

  }

  @Test
  def testGefxExport {

    for (job <- new EgoNetwork(parameters)) {
      job.buildNetwork
    }
    assertTrue(new File(outputPath + "/" + EgoNetwork.jobName + ".gexf").exists)

    val fileParameters = new NetworkParameters {
      override val source = "Gephi";
      override val connectionString = outputPath + "/" + EgoNetwork.jobName + ".gexf"
    }

    for (job <- new EgoNetwork(fileParameters)) {
      val (graph, metrics) = job.buildTheGraph
      assertEquals(4, graph.vertices.count)
      assertEquals(4, graph.edges.count)
      graph.vertices.foreach(x =>println( x._2.degree))
    }


  }

}
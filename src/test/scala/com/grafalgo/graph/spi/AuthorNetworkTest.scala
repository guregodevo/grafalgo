package com.grafalgo.graph.spi

import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test

import com.grafalgo.graph.spi.NetworkParameters;
import com.grafalgo.graph.spi.NetworkParameters._;
import com.typesafe.config.ConfigFactory
import com.grafalgo.graph.elastic.ElasticIntegrationTest
import com.grafalgo.graph.elastic.ElasticIntegrationTest._
import com.grafalgo.graph.network.author.ItemAuthorData
import com.grafalgo.graph.network.author.AuthorEdge
import com.grafalgo.graph.network.author.AuthorVertex
import com.grafalgo.graph.spi.util.scala.AutoResource.Arm
import com.grafalgo.graph.network.author.AuthorNetwork
import java.io.File
import com.grafalgo.graph.network.author.AuthorVertex
import com.grafalgo.graph.network.author.AuthorVertex
import org.apache.spark.graphx.Graph
import scala.reflect.ClassTag
import NetworkParameters._
import scala.collection.JavaConversions._
import com.grafalgo.graph.spi._

/**
 * NickNetwork creation tests
 *
 * @author guregodevo
 */
//TODO Complete tests
class AuthorNetworkTest extends ElasticIntegrationTest with ExportTest {

  private val cs = CONNECTION
  private val outputPath = "gexf/"

  private val SAMPLE_EDGES = List((1, 1, 2), (2, 2, 1), (2, 1, 3), (3, 1, 1), (3, 1, 2), (4, 1, 3))

  private type JobType = NetworkJob[ItemAuthorData, AuthorVertex, AuthorEdge]
  

  @Before
  def setUpContext {

    ConfigFactory.invalidateCaches
    System.setProperty("config.file", "./src/test/resources/authornetwork.conf")
    System.setProperty("spark.master", "local[2]")
    System.setProperty("spark.appName", "AuthorNetworkTest")
    try {
      deleteIndex
    }
    catch { case _: Exception => None }
    createIndex
    populateSampleItemData
  } 
  
  @Test
  def testGephiExport {
   gephiExport("AuthorNetwork", 4, 6, AuthorNetwork.jobName, outputPath)   
  }

  @Test
  def testJsonExport {
    jsonExport("AuthorNetwork", 4, 6, AuthorNetwork.jobName, outputPath)
  }
  
    @Test
  def testCsvExport {
    csvExport("AuthorNetwork", AuthorNetwork.jobName, outputPath)
  }  
  
  
  

  @Test
  def testAuthorNetworkInputRDD {

    for (job <- NetworkJob[JobType]("AuthorNetwork")) {

      val inputRDD = job.buildInputRDD.cache

      assertEquals(9, inputRDD.count)

      val output = inputRDD.sortByKey().collect

      for (j <- 0 to 8) {
        assertEquals(j + 1, output(j)._1)
      }
    }

  }

  @Test
  def testAuthorNetworkVertexRDD {

    for (job <- NetworkJob[JobType]("AuthorNetwork")) {

      val vertexRDD = job.buildGraph(job.buildInputRDD).vertices.cache

      assertEquals(4, vertexRDD.count)

      val output = vertexRDD.sortByKey().collect

      for (j <- 0 to 2) {
        assertEquals("author" + (j + 1), output(j)._2.nickName)
      }

      val author1 = output(0)._2
      val author2 = output(1)._2

      assertEquals(getDate("01-06-2015"), author1.firstItemDate)
      assertEquals(getDate("07-06-2015"), author1.lastItemDate)
      assertEquals(3, author1.urlNumber)
      assertEquals(2, author1.domainNumber)
      assertEquals(2, author2.urlNumber)
      assertEquals(1, author2.domainNumber)
    }

  }

  @Test
  def testAuthorNetworkEdgeRDD {

    for (job <- NetworkJob[JobType]("AuthorNetwork")) {

      val result = job.buildGraph(job.buildInputRDD).edges.collect
      assertEquals(6, result.size)
      val triplets = result.map(x => (x.srcId, x.attr.weight, x.dstId))
      SAMPLE_EDGES.foreach(x => assertTrue(triplets.contains(x)))
    }

  }

  @Test
  def testAuthorNetworkGraph {

    for (job <- NetworkJob[JobType]("AuthorNetwork")) {

      val graph = job.buildGraph(job.buildInputRDD).cache

      assertEquals(1, graph.vertices.filter { case (id, nick) => nick.nickName == "author1" }.count)
      assertEquals(1, graph.vertices.filter { case (id, nick) => nick.nickName == "author2" }.count)
      assertEquals(1, graph.vertices.filter { case (id, nick) => nick.nickName == "author3" }.count)
      assertEquals(1, graph.vertices.filter { case (id, nick) => nick.nickName == "author4" }.count)

      assertEquals(1, graph.edges.filter { e => e.attr.weight == 2 }.count)
      assertEquals(2, graph.edges.filter { e => e.attr.weight == 2 }.take(1)(0).srcId)
      assertEquals(1, graph.edges.filter { e => e.attr.weight == 2 }.take(1)(0).dstId)
    }
  }

  @Test
  def testSampleAuthorNetwork {

    try {
      deleteIndex
    }
    catch { case _: Exception => None }
    createIndex
    populateDataForSampleTest
    
    val config = ConfigFactory.parseMap(mapAsJavaMap(
      Map(SAMPLE_DATA -> "true",
        SAMPLE_SIZE -> "20")))

    val parameters = new NetworkParameters(config)

    for (job <- NetworkJob[JobType]("AuthorNetwork", parameters)) {

      val inputRDD = job.buildInputRDD.cache

      assertEquals(20, inputRDD.count)
   
    }

  }

}



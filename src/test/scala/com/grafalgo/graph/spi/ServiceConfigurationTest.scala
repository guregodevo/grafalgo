package com.grafalgo.graph.spi

import org.junit.Assert._
import org.junit.Test
import com.grafalgo.graph.spi.util.scala.AutoResource.Arm
import com.grafalgo.graph.spi._

class ServiceConfigurationTest {

  type JobType = NetworkJob[_, GraphVertex, GraphEdge]

  @Test
  def testJobProviderConfiguration {
    for (nickJob <- NetworkJob[JobType]("AuthorNetwork")) {
      assertNotNull(nickJob)
    }
  }

  @Test(expected = classOf[UnsupportedOperationException])
  def testNotExistingService {
    for (job <- NetworkJob[JobType]("notExistingService")) {
    }
  }

  @Test
  def testSourceProviderConfiguration {
    val nickSource: GraphDataSource[_, _, _, _] = GraphDataSource("AuthorNetworkElastic")
    assertNotNull(nickSource)
  }

  @Test
  def testExporterProviderConfiguration {
    val exporter: GraphExporter[_, _] = GraphExporter("AuthorNetworkGephiExporter")
    assertNotNull(exporter)
  }

}
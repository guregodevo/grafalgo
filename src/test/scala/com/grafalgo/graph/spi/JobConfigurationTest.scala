package com.grafalgo.graph.spi

import org.apache.spark.graphx.PartitionStrategy
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigException
import org.junit.Before

class JobConfigurationTest {

  @Before
  def clearConfigCaches {
    System.clearProperty("config.file")
    ConfigFactory.invalidateCaches
  }

  @Test(expected = classOf[ConfigException])
  def testMandatoryNotProvided {
    assertEquals("network", new NetworkParameters {}.network)
  }

  @Test(expected = classOf[UnsupportedOperationException])
  def testInvalidMetric {
    System.setProperty("config.file", "./src/test/resources/invalidmetric.conf")
    new NetworkParameters {}.metrics
  }

  @Test(expected = classOf[UnsupportedOperationException])
  def testInvalidFilter {
    System.setProperty("config.file", "./src/test/resources/invalidfilter.conf")
    new NetworkParameters {}.filters
  }

  @Test
  def testConfigurationDefaults {

    System.setProperty("config.file", "./src/test/resources/minimal.conf")
    val configuration = new NetworkParameters {}
    assertEquals("GephiExporter", configuration.exporter)
    assertEquals("Elastic", configuration.source)
    assertEquals(PartitionStrategy.CanonicalRandomVertexCut, configuration.partitionStrategy)
    assertEquals("AuthorNetwork", configuration.network)
  }

  @Test
  def testOverrideDefault {
    System.setProperty("config.file", "./src/test/resources/override.conf")
    ConfigFactory.invalidateCaches
    assertEquals(PartitionStrategy.RandomVertexCut, new NetworkParameters {}.partitionStrategy)
  }
}
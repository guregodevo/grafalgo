package com.grafalgo.graph.spi

import java.io.File

import org.junit.After
import org.junit.Assert

import com.grafalgo.graph.spi.util.scala.AutoResource.Arm
import com.typesafe.config.ConfigFactory

trait ExportTest {

  type U <: NetworkJob[Any, GraphVertex, GraphEdge]

  def gephiExport(serviceName: String, expectedVerticesCount: Int, expectedGraphEdge: Int, jobName: String, outputPath: String) {
    testExport(serviceName, "Gephi", "GephiExporter", ".gexf", expectedVerticesCount, expectedGraphEdge, jobName, outputPath, true)
  }

  def jsonExport(serviceName: String, expectedVerticesCount: Int, expectedGraphEdge: Int, jobName: String, outputPath: String) {
    testExport(serviceName, "Json", "JsonExporter", ".json", expectedVerticesCount, expectedGraphEdge, jobName, outputPath, true)
  }
  
  def csvExport(serviceName: String, jobName: String, outputPath: String) {
    testExport(serviceName, "Csv", "CsvExporter", ".csv", 0, 0, jobName + "_nodes", outputPath, false)
  }

  def testExport(serviceName: String, sourceConfig: String, exporterConfig: String, extension: String, expectedVerticesCount: Int, expectedGraphEdge: Int, jobName: String, outputPath: String, assertion:Boolean) {
    System.setProperty(NetworkParameters.EXPORTER_PROPERTY, exporterConfig)
    ConfigFactory.invalidateCaches    
    val parameters = new NetworkParameters()
    
    for (job <- NetworkJob[U](serviceName, parameters)) {
      job.buildNetwork
    }

    val filename = outputPath + "/" + jobName + extension
    
    Assert.assertTrue(s"file $filename should exist", new File(filename).exists)

    if (assertion) {
      val fileParameters = new NetworkParameters {
        override val source = sourceConfig;
        override val connectionString = outputPath + "/" + jobName + extension
      }
  
      
      for (job <- NetworkJob[U](serviceName, fileParameters)) {
        val (graph, metrics) = job.buildTheGraph
        Assert.assertEquals("all vertices should be exported", expectedVerticesCount, graph.vertices.count)
        Assert.assertEquals("all edges should be exported", expectedGraphEdge, graph.edges.count)
      }      
    }

  }
  
  @After
  def cleanExport() {
    System.setProperty(NetworkParameters.EXPORTER_PROPERTY, "GephiExporter")
  }  
  

}
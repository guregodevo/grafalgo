package com.grafalgo.graph.spi

import org.apache.spark.SparkContext
import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import NetworkParameters._
import com.grafalgo.graph.spi.util.scala.LazyLogging
import spark.jobserver.SparkJob
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
/**
 * Entry point for the spark job-server
 */
class JobServerLauncher extends SparkJob with LazyLogging {

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    logger.info("Job server activation")
    val job = NetworkJob[NetworkJob[_, _ <: GraphVertex, _ <: GraphEdge]](jobConfig.getString(NETWORK_PROPERTY), new NetworkParameters(jobConfig))
    job.setContext(sc)
    job.buildNetwork
  }

  override def validate(sc: SparkContext, jobConfig: Config): SparkJobValidation = {
    logger.info("Job server validation")
    try {
      jobConfig.getString(NETWORK_PROPERTY)
      jobConfig.getString(CONNECTION_STRING_PROPERTY)
      jobConfig.getString(OUTPUTPATH_PROPERTY)
    }
    catch {
      case e: ConfigException => { logger.error("Missing mandatory parameters: ", e); SparkJobInvalid }
    }
    SparkJobValid
  }

}
package com.grafalgo.graph.network.ego

import com.grafalgo.graph.spi.NetworkParameters
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.Map

class EgoNetworkParameters(jobConfig: Config) extends NetworkParameters(jobConfig) {

  import EgoNetworkParameters._

  def this() = this(ConfigFactory.empty)

  def this(params: NetworkParameters) = this(if (params == null) ConfigFactory.empty else params.config)

  override protected def getDefaultConfig = super.getDefaultConfig.withFallback(ConfigFactory
    .parseMap(mapAsJavaMap(Map(RELATIONS -> "25"))))

  val relations = config.getInt(RELATIONS)

}

object EgoNetworkParameters extends EgoNetworkParameters {
  final val RELATIONS = "network.ego.relations"
}
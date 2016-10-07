package com.grafalgo.graph.spi

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import com.typesafe.config.ConfigFactory
import com.grafalgo.graph.spi.util.scala.LazyLogging
import com.typesafe.config.Config

/**
 * A Service Provider definition
 */
trait ServiceProvider[T] extends LazyLogging {
  private val services = mutable.Map[String, ServiceFactory[T]]()
    .withDefault(key => throw new UnsupportedOperationException("Service " + key + " is not registered"))
  private val name = "name"
  private val service = "service"
  protected val serviceRoot: String
  protected val fileName: String

  protected val configObject = ConfigFactory.load(fileName)

  configObject.getObjectList(serviceRoot).asScala.foreach { x =>
    registerService(x.get(name).unwrapped.toString,
      Class.forName(x.get(service).unwrapped.toString).newInstance.asInstanceOf[ServiceFactory[T]])
  }

  private def registerService(name: String, factory: ServiceFactory[T]): Unit = services(name) = factory

  def apply[V <: T](serviceName: String, parameters: NetworkParameters = new NetworkParameters()): V = services(serviceName).service(parameters).asInstanceOf[V]

  def getDefaultServiceName: String

}

/*
 * Simple service factory interface
 */
trait ServiceFactory[V] {
  def service(parameters: NetworkParameters): V
}


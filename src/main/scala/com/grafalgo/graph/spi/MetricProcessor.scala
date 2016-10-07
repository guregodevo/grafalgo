package com.grafalgo.graph.spi
import spire.compat._
import spire.implicits._
import spire.math._

trait MetricProcessor[T, V <: GraphVertex] extends Serializable {

  implicit def metricOperations: ScalarOperations[T]

  def get(node: V): T
  def put(value: T, node: V)
  def putString(value: String, node: V) = put(metricOperations.getValueFromString(value), node)
  def name: String
}
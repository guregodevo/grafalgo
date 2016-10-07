package com.grafalgo.graph.spi

import com.grafalgo.graph.spi.util.scala.LazyLogging
import scala.{ specialized => spec }
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

abstract class ClusterMetricProcessor[@spec(Int, Long) T: ScalarOperations](name:String) extends AbstractClusterProcessor[T, GraphVertex](name) with LazyLogging
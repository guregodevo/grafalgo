package com.grafalgo.graph.spi

import scala.reflect.ClassTag
import scala.{ specialized => spec }
import org.apache.spark.rdd.RDD
import ScalarOperations._
import spire.compat._
import spire.implicits._
import spire.math._
import ScalarProcessor._

/*
   * Class that provides a set of common processing operations for the different metric value types
   */
abstract class ScalarMetricProcessor[@spec(Int, Long, Double) T: ScalarOperations: Numeric](name:String, minString: String = null, maxString: String = null)
    extends AbstractScalarProcessor[T, GraphVertex](name, minString, maxString)

package com.grafalgo.graph.spi

import spire.implicits._
import spire.math._
import spire.compat._
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import com.grafalgo.graph.spi.util.scala.LazyLogging
import scala.collection.immutable.SortedSet
import scala.{ specialized => spec }

/**
 * Trait defining the api needed for processing the scalar values in the Vertex RDDs
 */
trait ScalarProcessor[@spec(Int, Long, Double) T, V <: GraphVertex] extends MetricProcessor[T,V] with LazyLogging {

  import ScalarProcessor._
  
  implicit def numeric: Numeric[T]

  def min(minString: String) = metricOperations.getMinValue(minString)
  def max(maxString: String) = metricOperations.getMinValue(maxString)

  def min: T
  def max: T

  def getAsDouble(node: V): Double = numeric.toDouble(get(node))
  def filter(node: V) = { val v = get(node); numeric.gteqv(v, min) && numeric.lteqv(v, max) }

  override def equals(that: Any): Boolean =
    that match {
      case that: ScalarProcessor[T, _] => this.max == that.max && this.min == that.min && this.name.equals(that.name)
      case _ => false
    }

  override def hashCode: Int = {
    max.hashCode + min.hashCode + name.hashCode
  }

  override def toString = "Processor: " + name + ",min: " + min + ",max: " + max

  def getPercentiles[V1 <: V](rdd: RDD[V1])(implicit tag: ClassTag[T]): (Array[T], Double, Double) = getQuantiles(rdd, PERCENTILES)
  def getQuantiles[V1 <: V](rdd: RDD[V1], quantiles: Int)(implicit tag: ClassTag[T]): (Array[T], Double, Double) = getQuantiles(quantiles,
    rdd.map(get(_)))
  def getHistogram[V1 <: V](rdd: RDD[V1], bins: Int, min: Double, max: Double)(implicit tag: ClassTag[T]): Array[Long] = getHistogram(bins, min, max,
    rdd.map(get(_)))
  def getHistogram[V1 <: V](rdd: RDD[V1], min: Double, max: Double)(implicit tag: ClassTag[T]): Array[Long] = getHistogram(rdd, BINS, min, max)
  def getTop[V1 <:V](rdd: RDD[(Long,V1)], top:Int)(implicit tag: ClassTag[T]):Array[(Long,V1)] = rdd.top(top)(Ordering[T].on(x => get(x._2)))

  /*
   * Get the quantiles as an array of elements. Each element of the array marks the value at the start of the nth quantile
   * If the number of quantiles is multiple of 4, return also the extreme range limits as 3 times the inter quartile range
   */
  //TODO Strict percentile generation is a quite heavy operation (it requires sorting the RDD). Evaluate approximations (T-Digest)
  private def getQuantiles(quantiles: Int, rdd: RDD[T])(implicit tag: ClassTag[T]): (Array[T], Double, Double) = {
    val interval = rdd.count / quantiles
    if (interval < 1) { logger.warn("Dataset too small for generating the requested quantiles"); return (Array.empty, 0, 0) }
    val sorted = rdd.sortBy(x => x).zipWithIndex().map(_.swap)
    val indicesToRequest = for (i <- 0 to (quantiles - 1)) yield (interval * i)

    val result = sorted.filter(x => SortedSet(indicesToRequest: _*).contains(x._1)).collect.sortBy(_._1).map(_._2)

    var minOuterFence: Double = 0.0
    var maxOuterFence: Double = 0.0

    if (result.length > 0 && quantiles % 4 == 0) {
      val firstQuartile = quantiles / 4
      val thirdQuartile = 3 * firstQuartile
      minOuterFence = result(firstQuartile).toDouble
      maxOuterFence = result(thirdQuartile).toDouble
      val outlierRange = 3 * Math.abs(maxOuterFence - minOuterFence)
      minOuterFence = minOuterFence - outlierRange
      maxOuterFence = maxOuterFence + outlierRange
    }

    (result, minOuterFence, maxOuterFence)
  }

  /*
   * Histogram using the RDD[Double] api.
   */
  private def getHistogram(buckets: Int, min: Double, max: Double, rdd: RDD[T]): Array[Long] = {
    val range = (max - min) / buckets
    val b = ((for (i <- 0 to buckets - 1) yield (min + i * range)) :+ max).toArray

    rdd.map(_.toDouble).histogram(b, false)
  }
}

object ScalarProcessor {
  final val BINS: Int = 100;
  final val PERCENTILES: Int = 100;
}

/*
 * Convenience abstract class materializing the type class implicits
 */
abstract class AbstractScalarProcessor[T: ScalarOperations: Numeric, V <: GraphVertex](override val name: String, minString: String = null, maxString: String = null) extends ScalarProcessor[T, V] {

  override val min: T = min(minString)
  override val max: T = max(maxString)

  override lazy val numeric: Numeric[T] = implicitly[Numeric[T]]
  override lazy val metricOperations = implicitly[ScalarOperations[T]]

}
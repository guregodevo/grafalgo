package com.grafalgo.graph.spi

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import scala.{ specialized => spec }

import com.grafalgo.graph.spi.util.scala.LazyLogging

trait ClusterProcessor[@spec(Int, Long) T, V <: GraphVertex] extends MetricProcessor[T,V] with LazyLogging {
  
  /**
   * Get the id and size of the k biggest clusters
   */
  def getTopClusters[V1 <: V](rdd: RDD[V], k: Int = 10)(implicit tag: ClassTag[T]):(Long,Array[(T,Long)]) = { 
    
    val clusterRDD = rdd.map(x => (get(x), 1L)).reduceByKey(_ + _).cache
    val count = clusterRDD.count
    val clusters = clusterRDD.takeOrdered(k)(Ordering[Long].reverse.on(x => x._2))
    clusterRDD.unpersist(false)
    
    (count, clusters)
  }
}

/*
 * Convenience abstract class materializing the type class implicits
 */
abstract class AbstractClusterProcessor[T: ScalarOperations, V <: GraphVertex](override val name: String) extends ClusterProcessor[T, V] {

  override lazy val metricOperations = implicitly[ScalarOperations[T]]

}
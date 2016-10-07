package com.grafalgo.graph.spi

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import com.grafalgo.graph.spi.GraphMetrics._

/**
 * This trait defines the source of data for the initial input RDD
 *
 * @author guregodevo
 */
trait GraphDataSource[T, I, V <: GraphVertex, E <: GraphEntity] extends Serializable {

  /*
   * Gets the initial RDD using the Spark Context
   */
  protected def getRawInputRDD(sc: SparkContext, parameters:NetworkParameters): RDD[T]

  /*
   * Gets the pair of id, Item data object from the source format
   */
  protected def getItemData(rawObject: T): Tuple2[Long, I] =
    throw new UnsupportedOperationException("This datasource does not provide a raw document input for generating the graph. Try using the load interface.")

  /*
   * Builds a RDD of Item data objects from the initial format RDD
   */
  def buildItemInputRDD(sc: SparkContext, parameters:NetworkParameters): RDD[(Long, I)] = getRawInputRDD(sc, parameters).map(getItemData)

  /*
   * Returns the previously stored vertices and edges RDDs
   */
  def loadGraphRDDs(sc: SparkContext, parameters:NetworkParameters): (RDD[(Long, V)], RDD[Edge[E]], Array[GraphMetric]) =
    throw new UnsupportedOperationException("This datasource does not support loading stored vertices and edges.")
  
  /*
   * Check if the vertex and edges can be directly loaded
   */
  def directLoad:Boolean

}

/*
 * Companion Factory Provider
 */
object GraphDataSource extends ServiceProvider[GraphDataSource[_, _, _, _]] {
  override lazy val fileName = "job-service"
  override lazy val serviceRoot = "source"

  override def getDefaultServiceName = configObject.getString("defaultSource")
}

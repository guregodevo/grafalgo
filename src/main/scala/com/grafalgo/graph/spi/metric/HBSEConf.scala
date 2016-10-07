package com.grafalgo.graph.spi.metric


import java.util.Date
import org.apache.spark.SparkConf
import com.grafalgo.graph.spi.NetworkParameters

/**
 * Stores the configuration values for the HighBetweennessCore Object.
 *
 * @param shortestPathPhases The number of shortest path phases to run through.
 * @param setStabilityDifference The delta value that must be met for the set to be stable.
 * @param setStabilityDifferenceCount The number of times the set stability counter must be met.
 * @param betweennessSetMaxSize The size of the betweenness set.
 * @param pivotBatchSize The number of pivots to select per run.
 * @param initialPivotBatchSize The number of pivots to use in the first run.
 * @param pivotSelectionRandomSeed The random seed to pass to the takeSample.
 * @param totalNumberOfPivots The total number of pivots to use in an entire run.
 * @param setStabilityCounter A counter that keeps track of the number of times the betweenness set has been stable.
 */
case class HBSEConf(
                     var shortestPathPhases: Int= 1,
                     var setStabilityDifference: Int = 2,
                     var setStabilityDifferenceCount: Int = 0,
                     var betweennessSetMaxSize: Int = 20,
                     var pivotBatchSize: Int = 16,
                     var initialPivotBatchSize: Int = 16,
                     var pivotSelectionRandomSeed: Long = (new Date).getTime,
                     var maxtotalNumberOfPivots: Int = 64,//TODO increase it. The larger number of pivots in turn results in higher accuracy.
                     var setStabilityCounter: Int = 0) extends Serializable {

  def this(c: NetworkParameters) = {  
    this()
    this.pivotBatchSize = c.betweennessBatchSize
    this.initialPivotBatchSize = c.betweennessBatchSize
    this.maxtotalNumberOfPivots = c.betweennessMaxPivots
  }
  
  def increaseSetStabilityCount() = {
    this.setStabilityCounter = this.setStabilityCounter + 1
  }

  def resetSetStabilityCount() = {
    this.setStabilityCounter = 0
  }

}
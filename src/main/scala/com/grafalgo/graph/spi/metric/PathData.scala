package com.grafalgo.graph.spi.metric


/**
 * PathData object for storing path data from a pivot and through a node.
 *
 * @param distance Distance from the pivot to message source.
 * @param pivotSource Source of the initial message.
 * @param messageSource Source of the node that forwarded that message to you.
 * @param numberOfPaths Number of paths it took to get there.
 */
class PathData(private var distance: Long, private var pivotSource: Long, private var messageSource: Long, private var numberOfPaths: Long) extends Serializable {
  /**
   * Returns the distance value.
   * @return value of distance.
   */
  def getDistance = this.distance

  /**
   * Returns the pivot who send the initial message.
   * @return value of pivotSource.
   */
  def getPivotSource = this.pivotSource

  /**
   * Returns the node that forwarded the message.
   * @return value of messageSource.
   */
  def getMessageSource = this.messageSource

  /**
   * Returns the number of shortest paths for this object.
   * @return value of numberOfPaths.
   */
  def getNumberOfShortestPaths = this.numberOfPaths


}

/**
 * Helper object for instantiating a PathData object.
 */
object PathData {
  /**
   * Creates a PathData object intended for the shortest path run.
   * @param pivotSource Pivot Source.
   * @param messageSource Who sent the message.
   * @param distance Distance from pivot source to message source.
   * @param numPaths Number of Shortest paths.
   * @return
   */
  def createShortestPathMessage(pivotSource: Long, messageSource: Long, distance: Long, numPaths: Long) = new PathData(distance, pivotSource, messageSource, numPaths)
}
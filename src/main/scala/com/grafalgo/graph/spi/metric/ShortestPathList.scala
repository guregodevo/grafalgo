package com.grafalgo.graph.spi.metric

import scala.collection.mutable

/**
 * Object for storing the number of shortest paths through a node.
 * @param distance Shortest distance in the shortest path map.
 * @param predecessorPathCountMap Map for storing a nodes predecessors in the format of (vertexId, edge weight).
 */
class ShortestPathList(private var distance: Long, private var predecessorPathCountMap: mutable.HashMap[Long, Long]) extends Serializable {

  /**
   * Default Constructor.
   * @return ShortestPathList Object.
   */
  def this() = this(Long.MaxValue, new mutable.HashMap[Long, Long])

  /**
   * Constructor that accepts a path data object.
   * @param pathData PathData object to instantiate with.
   * @return ShortestPathList object.
   */
  def this(pathData: PathData) = {
    this()
    this.distance = pathData.getDistance
    this.predecessorPathCountMap.put(pathData.getMessageSource, pathData.getNumberOfShortestPaths)
  }

  /**
   * Returns the predecessorPathCountMap.
   * @return value of predecessorPathCountMap.
   */
  def getPredecessorPathCountMap = this.predecessorPathCountMap

  /**
   * Returns the distance of the shortest path list.
   * @return value of distance.
   */
  def getDistance = this.distance

  /**
   * Gets the shortest path count.
   * @return Accumulated total of the shortest path count.
   */
  def getShortestPathCount = this.predecessorPathCountMap.foldLeft(0L)((total: Long, mapItem: (Long, Long)) => total + mapItem._2)


  /**
   * Updates the distance from a pivot to this vertex.
   * @param pathData Object that encapsulates the updated path data.
   * @return True if updated.
   */
  def update(pathData: PathData): Boolean = {
    var updated: Boolean = false
    if (this.distance == pathData.getDistance) {
      val oldNumberOfShortestPaths = this.predecessorPathCountMap.getOrElse(pathData.getMessageSource, Long.MinValue)
      updated = oldNumberOfShortestPaths != pathData.getNumberOfShortestPaths
      if (updated) {
        this.predecessorPathCountMap.put(pathData.getMessageSource, pathData.getNumberOfShortestPaths)
      }
    } else if (pathData.getDistance < this.distance) {
      this.distance = pathData.getDistance
      this.predecessorPathCountMap.clear()
      this.predecessorPathCountMap.put(pathData.getMessageSource, pathData.getNumberOfShortestPaths)
      updated = true
    } else {
      updated = false
    }
    updated
  }


}
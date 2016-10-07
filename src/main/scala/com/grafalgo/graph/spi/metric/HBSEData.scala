package com.grafalgo.graph.spi.metric
import scala.collection.mutable

/**
 * Object that stores the required data for the HBSE Run.
 *
 * @param pivotPathMap A hash map of accumulated PathData from the Pivots.
 * @param partialDependencyMap A hash map of dependency accumulation.
 * @param approximateBetweenness Approximate Betweenness value.
 */
class HBSEData(private var pivotPathMap: mutable.HashMap[Long, ShortestPathList], private var partialDependencyMap: mutable.HashMap[Long, PartialDependency], private var approximateBetweenness: Double)
  extends Serializable  {

  /**
   * Constructor that accepts an initial betweenness value.
   * @param betweenness Betweenness value to instantiate.
   * @return HBSEData object.
   */
  def this(betweenness: Double) = this(new mutable.HashMap[Long, ShortestPathList], new mutable.HashMap[Long, PartialDependency], betweenness)

  /**
   * Default Constructor
   * @return HBSEData object.
   */
  def this() = this(0.0)


  /**
   * Add or updates path data in the path data map.
   * @param pathData The new path data.
   * @return A shortest path list if it was updated or else null.
   */
  def addPathData(pathData: PathData): ShortestPathList = {
    var list: ShortestPathList = null
    val source: Long = pathData.getPivotSource
    if (!pivotPathMap.contains(source)) {
      list = new ShortestPathList(pathData)
      this.pivotPathMap.put(source, list)
    }
    else {
      list = pivotPathMap.get(source).get
      list = if (list.update(pathData)) list else null
    }
    list
  }

  /**
   * Return the approxBetweenness value.
   * @return value of approxBetweenness.
   */
  def getApproximateBetweenness = this.approximateBetweenness


  /**
   * Returns the path data map.
   * @return pivotPathMap
   */
  def getPathDataMap = this.pivotPathMap

  /**
   * Adds or updates a partial dependency to the hash map.
   * @param src Node that has the dependency.
   * @param dependency Value to add to the map.
   * @param numberOfSuccessors numberOfSuccessors for this node.
   * @return The partial dependency object that was updated.
   */
  def addPartialDependency(src: Long, dependency: Double, numberOfSuccessors: Int): PartialDependency = {
    val current: PartialDependency = this.partialDependencyMap.getOrElse(src, new PartialDependency)
    current.accumulateDependency(dependency)
    current.accumulateSuccessors(numberOfSuccessors)
    this.partialDependencyMap.put(src, current)
    current
  }

  /**
   * Returns the partial dependency map.
   * @return value of partialDependencyMap.
   */
  def getPartialDependencyMap = this.partialDependencyMap


}

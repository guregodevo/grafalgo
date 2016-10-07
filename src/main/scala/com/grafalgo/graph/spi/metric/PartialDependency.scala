package com.grafalgo.graph.spi.metric

/**
 * An object for encapsulating a partial dependency.
 *
 * @param numberOfSuccessors The number of successors for this dependency.
 * @param accumulatedDependency The dependency weight on this object.
 */
class PartialDependency(private var numberOfSuccessors: Int, private var accumulatedDependency: Double) extends Serializable {

  /**
   * Default constructor.
   * @return PartialDependency Object.
   */
  def this() = this(0, 0.0)

  /**
   * Return the dependency.
   * @return Dependency.
   */
  def getDependency = this.accumulatedDependency

  /**
   * Returns the number of successors.
   * @return Successors.
   */
  def getSuccessors = this.numberOfSuccessors

  /**
   * Set the number of successors.
   * @param successors value to set the successors to.
   */
  def setSuccessors(successors: Int) = this.numberOfSuccessors = successors

  /**
   * Set the dependency.
   * @param dependency value to set the dependency to.
   */
  def setDependency(dependency: Double) = this.accumulatedDependency = dependency

  /**
   * Accumulates the successor object.
   * @param valueToAdd value to add to the successors.
   */
  def accumulateSuccessors(valueToAdd: Int) = this.numberOfSuccessors += valueToAdd

  /**
   * Accumulates the dependency value.
   * @param valueToAdd value to add to the dependencies.
   */
  def accumulateDependency(valueToAdd: Double) = this.accumulatedDependency += valueToAdd


}
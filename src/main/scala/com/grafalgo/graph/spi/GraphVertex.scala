package com.grafalgo.graph.spi

import GraphMetrics._

//FIXME Make all metrics immutable, refactorize this code
trait GraphVertex {

  var degree: Int = _
  var inDegree: Int = _
  var outDegree: Int = _
  var weightedDegree: Int = _
  var weightedInDegree: Int = _
  var weightedOutDegree: Int = _
  var eigenCentrality: Double = _
  var betweennessCentrality: Double = _
  var pageRank: java.lang.Double = _
  var modularity: Long = _
  var component: Long = _

  def withMetrics[V <: GraphVertex](v: GraphVertex, metrics: Array[GraphMetric]): V = {
    val result = copyVertex
    result.degree = this.degree
    result.inDegree = this.inDegree
    result.outDegree = this.outDegree
    result.weightedDegree = this.weightedDegree
    result.weightedInDegree = this.weightedInDegree
    result.weightedOutDegree = this.weightedOutDegree
    result.eigenCentrality = this.eigenCentrality
    result.betweennessCentrality = this.betweennessCentrality
    result.pageRank = this.pageRank
    result.modularity = this.modularity
    result.component = this.component
    
    for (m <- metrics) {
      m match {
        case GraphMetrics.DEGREE => result.degree = v.degree
        case GraphMetrics.INDEGREE => result.inDegree = v.inDegree
        case GraphMetrics.OUTDEGREE => result.outDegree = v.outDegree
        case GraphMetrics.WEIGHTEDDEGREE => result.weightedDegree = v.weightedDegree
        case GraphMetrics.WEIGHTEDINDEGREE => result.weightedInDegree = v.weightedInDegree
        case GraphMetrics.WEIGHTEDOUTDEGREE => result.weightedOutDegree = v.weightedOutDegree
        case GraphMetrics.EIGENCENTRALITY => result.eigenCentrality = v.eigenCentrality
        case GraphMetrics.BETWEENNESSCENTRALITY => result.betweennessCentrality = v.betweennessCentrality
        case GraphMetrics.PAGERANK => result.pageRank = v.pageRank
        case GraphMetrics.MODULARITY => result.modularity = v.modularity
        case GraphMetrics.COMPONENT => result.component = v.component
      }
    }

    result.asInstanceOf[V]
  }

  def copyVertex: GraphVertex

}
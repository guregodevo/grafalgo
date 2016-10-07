package com.grafalgo.graph.spi

case class GraphMetricsHolder extends GraphVertex {
  def withMetric(degree: java.lang.Integer = this.degree, inDegree: java.lang.Integer = this.inDegree, outDegree: java.lang.Integer = this.outDegree,
    weightedDegree: java.lang.Integer = this.weightedDegree, weightedInDegree: java.lang.Integer = this.weightedInDegree,
    weightedOutDegree: java.lang.Integer = this.weightedOutDegree, eigenCentrality: java.lang.Double = this.eigenCentrality,
    pageRank: java.lang.Double = this.pageRank, modularity: java.lang.Long = this.modularity, component: java.lang.Long = this.component,
    betweennessCentrality: java.lang.Double = this.betweennessCentrality) = {

    val result = new GraphMetricsHolder()

    result.degree = degree
    result.inDegree = inDegree
    result.outDegree = outDegree
    result.weightedDegree = weightedDegree
    result.weightedInDegree = weightedInDegree
    result.weightedOutDegree = weightedOutDegree
    result.eigenCentrality = eigenCentrality
    result.betweennessCentrality = betweennessCentrality
    result.pageRank = pageRank
    result.modularity = modularity
    result.component = component

    result
  }
  
   override def copyVertex:GraphVertex = this.copy()
}
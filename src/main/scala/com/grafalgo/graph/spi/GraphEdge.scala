package com.grafalgo.graph.spi


/**
 * Mixin trait for weighted networks
 * 
 * @author guregodevo
 */
trait GraphEdge extends GraphEntity {
  val weight: Int
}
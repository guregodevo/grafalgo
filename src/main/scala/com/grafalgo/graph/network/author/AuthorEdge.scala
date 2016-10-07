package com.grafalgo.graph.network.author

import com.grafalgo.graph.spi.GraphEdge
import com.grafalgo.graph.spi.GraphEntity
import com.grafalgo.graph.spi._
import scala.collection.mutable.ArrayBuffer

/**
 * Class storing the properties of the nick network edges
 *
 * @author guregodevo
 */
case class AuthorEdge(
  override val weight: Int,
  val firstItemBody: String,
  val lastItemBody: String,
  val firstItemLink: String,
  val lastItemLink: String,
  val firstItemDate: Long,
  val lastItemDate: Long) extends GraphEdge
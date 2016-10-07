package com.grafalgo.graph.network.ego

import com.grafalgo.graph.spi.GraphVertex
import scala.collection._

case class AuthorVertex(val nickName: String,
  val nickId: Long,
  val activity: Long,
  val rating: Long,
  val country: String,
  val location: String,
  val webSite: String,
  val firstItemDate: Long,
  val lastItemDate: Long,
  val urlNumber: Int,
  val domainNumber: Int,
  val domainSet: mutable.Set[String],
  val urlSet: mutable.Set[Long],
  val parentData: Boolean = false) extends GraphVertex{
  override def copyVertex:GraphVertex = this.copy()
}
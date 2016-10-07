package com.grafalgo.graph.network.author

import com.grafalgo.graph.spi.GraphVertex

/**
 * Class storing the properties of the author network vertex
 *
 * @author guregodevo
 */
case class AuthorVertex(val nickName: String,
    val activity: Long,
    val rating: Long,
    val country: String,
    val location: String,
    val webSite: String,
    val firstItemDate: Long,
    val lastItemDate: Long,
    val urlNumber: Int,
    val domainNumber: Int,
    val parentData: Boolean = false) extends GraphVertex{
  override def copyVertex:GraphVertex = this.copy()
}
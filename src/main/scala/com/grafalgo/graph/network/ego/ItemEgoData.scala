package com.grafalgo.graph.network.ego

/*
 * input data structure containing required nicks information
 * 
 * @author guregodevo
 */
class ItemEgoData(
  val itemParentId: Long,
  val itemLink: String,
  val itemBody: String,
  val itemPublishDate: Long,
  val publisherId: Long,
  val publisherDescription: String,
  val nickId: Long,
  val nickName: String,
  val nickActivity: Long,
  val nickRating: Long,
  val personLocation: String,
  val personCountry: String,
  val personWebsite: String,
  val domains: Array[String],
  val urls: Array[Long],
  val parentPublishDate: Long,
  val parentNickId: Long,
  val parentNickName: String,
  val parentNickActivity: Long,
  val parentNickRating: Long,
  val mentions:Array[MentionData],
  val replies:Array[MentionData])
  
class MentionData(
    val authorId:Long,
    val authorName:String,
    val authorActivity:Long,
    val authorRating:Long)

class ReferenceData(
  val referenceHost: String,
  val referenceId: Long)
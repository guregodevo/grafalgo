package com.grafalgo.graph.network

  
package object ego { 
  final val NAME = "NAME"
  final val AUTHORID = "NICKID"
  final val WEBSITE = "PERSONAL-WEBSITE"
  final val ACTIVITY = "ALL-NICK-ACTIVITY-EVER"
  final val RATING = "NICK-FOLLOWERS"
  final val COUNTRY = "COUNTRY"
  final val LOCATION = "LOCATION"
  final val URLS = "Urls Sent"
  final val DOMAINS = "Domains Sent"
  final val TYPE = "Type"
  final val TYPE_VAL = "user"
  final val FIRSTDATE = "FIRST PUBDATE"
  final val LASTDATE = "LAST PUBDATE"
  final val LINKS = "LINKS"
  final val ID = "id"
  final val WEIGHT = "weight"
  final val LABEL = "label"
  final val SOURCE = "source"
  final val TARGET = "target"
  
  final val NAME_ATTR = "<attribute id=\"NAME\" title=\"NAME\" type=\"string\"/>"
  final val WEB_ATTR = "<attribute id=\"PERSONAL-WEBSITE\" title=\"PERSONAL-WEBSITE\" type=\"string\"/>"
  final val ACTIVITY_ATTR = "<attribute id=\"ALL-NICK-ACTIVITY-EVER\" title=\"ALL-NICK-ACTIVITY-EVER\" type=\"integer\"/>"
  final val FOLLOWERS_ATTR = "<attribute id=\"NICK-FOLLOWERS\" title=\"NICK-FOLLOWERS\" type=\"integer\"/>"
  final val COUNTRY_ATTR = "<attribute id=\"COUNTRY\" title=\"COUNTRY\" type=\"string\"/>"
  final val LOCATION_ATTR = "<attribute id=\"LOCATION\" title=\"LOCATION\" type=\"string\"/>"
  final val URLS_ATTR = "<attribute id=\"Urls Sent\" title=\"Urls Sent\" type=\"integer\"/>"
  final val DOMAINS_ATTR = "<attribute id=\"Domains Sent\" title=\"Domains Sent\" type=\"integer\"/>"
  final val FIRST_DATE_ATTR = "<attribute id=\"FIRST PUBDATE\" title=\"FIRST PUBDATE\" type=\"string\"/>"
  final val LAST_DATE_ATTR = "<attribute id=\"LAST PUBDATE\" title=\"LAST PUBDATE\" type=\"string\"/>"
  final val TYPE_ATTR = "<attribute id=\"Type\" title=\"Type\" type=\"string\"/>"
  
  final val LINKS_ATTR = "<attribute id=\"LINKS\" title=\"LINKS\" type=\"string\"/>"
}
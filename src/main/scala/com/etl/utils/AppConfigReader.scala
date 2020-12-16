package com.etl.utils

import java.util.Properties

import scala.io.Source

object AppConfigReader {

  case class AppConfig(events:String, purchases:String, campaignsRevenueOutputTable:String, channelEngagementOutputTable:String)

  def getAppConfig(path:String ="/config/application.properties", env:String):AppConfig = {
    val url = getClass.getResource(path)
    val properties: Properties = new Properties()
    properties.load(Source.fromURL(url).bufferedReader())
    val events  = if (env.equalsIgnoreCase("test")) getClass.getResource(properties.getProperty("events", null)).getPath
                  else properties.getProperty("events", null)
    val purchases = if (env.equalsIgnoreCase("test")) this.getClass.getResource(properties.getProperty("purchases", null)).getPath
                    else properties.getProperty("purchases", null)
    val campaignsRevenueOutputTable  = properties.getProperty("campaignsRevenueOutputTable", null)
    val channelEngagementOutputTable = properties.getProperty("channelEngagementOutputTable", null)

    AppConfig(events, purchases, campaignsRevenueOutputTable, channelEngagementOutputTable)
  }
}

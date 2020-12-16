package com.etl

import com.etl.context.DataContextBuilder
import com.etl.context.DataContextBuilder.DataContext
import com.etl.reports.SQLPipelineLogic._
import com.etl.utils.AppConfigReader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

object App extends Logging{

  /**
    environment_name = LOCAL
    batch_id = 20201216000000
    config_file = application.properties
    app_name = click-stream-app
  **/
  def main(args: Array[String]): Unit = {

    if(args.length < 4) throw new IllegalArgumentException("args failure")

    val sparkSession = SparkSession.builder()
      .master("master")
      .appName(args(3))
      .enableHiveSupport()
      .getOrCreate()

    val appConfig = AppConfigReader.getAppConfig(args(2), args(0))
    log.info(s"Job Running config = $appConfig")

    log.info(s"Building the data context.")
    val dataContext: DataContext = DataContextBuilder.gather(sparkSession, appConfig)

    log.info(s"Create sessions from event using SQLPipelineLogic implementation. ")
    val sessions = getSession(dataContext.events)

    log.info(s"Combine session data set with purchases.")
    val purchasesDetails = getPurchasesDetails(sessions, dataContext.purchases)

    log.info(s"Get Top Campaigns by revenue.")
    val topCampaigns = getTopCampaignsByRevenue(purchasesDetails)

    log.info(s"Get Top Channels  by Engagement.")
    val topChannels = getTopChannelsByEngagement(purchasesDetails)

    log.info(s"Writing to output table ${appConfig.campaignsRevenueOutputTable} ")
    topCampaigns.write.mode(SaveMode.Overwrite).saveAsTable(appConfig.campaignsRevenueOutputTable)

    log.info(s"Writing to output table ${appConfig.channelEngagementOutputTable} ")
    topChannels.write.mode(SaveMode.Overwrite).saveAsTable(appConfig.channelEngagementOutputTable)
  }
}

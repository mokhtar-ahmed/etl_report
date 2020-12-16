package com.etl.utils

import java.sql.Timestamp
import com.etl.metadata.MetaData._
import com.etl.utils.AppConfigReader.AppConfig

object TestsConstants {


  val eventsPath = getClass.getResource("/data/events.csv").getPath
  val ordersPath = getClass.getResource("/data/purchases.csv").getPath
  val campaignsRevenueOutputTable="campaign_revenue"
  val channelEngagementOutputTable="channel_engagement"

  val config = AppConfig(eventsPath, ordersPath, campaignsRevenueOutputTable, channelEngagementOutputTable)

  val purchaseDetailsExpected = Seq(
    PurchaseDetails( "u3_1" , "u3"  ,  "p3", "cmp1", "Google Ads", true , 600.0, Timestamp.valueOf("2019-01-02 02:00:00")),
    PurchaseDetails( "u3_2" , "u3"  ,  "p4", "cmp2", "Yandex Ads", false, 700.0, Timestamp.valueOf("2019-01-02 02:00:00")),
    PurchaseDetails( "u3_2" , "u3"  ,  "p5", "cmp2", "Yandex Ads", true , 100.0, Timestamp.valueOf("2019-01-02 02:00:00")),
    PurchaseDetails( "u3_3" , "u3"  ,  "p6", "cmp2", "Yandex Ads", true , 400.0, Timestamp.valueOf("2019-01-02 02:00:00")),
    PurchaseDetails( "u1_0" , "u1"  ,  "p1", "cmp1", "Google Ads", true , 400.0, Timestamp.valueOf("2019-01-02 02:00:00")),
    PurchaseDetails( "u2_0" , "u2"  ,  "p2", "cmp1", "Yandex Ads", false, 300.0, Timestamp.valueOf("2019-01-02 02:00:00"))
  )

  val campaignRevenuesExpected = Seq(
    CampaignRevenue("cmp1", 1000.0),
    CampaignRevenue("cmp2", 500.0))


  val channelsEngagementExpected = Seq(
    ChannelEngagement("Yandex Ads", 3),
    ChannelEngagement("Google Ads", 2))

}

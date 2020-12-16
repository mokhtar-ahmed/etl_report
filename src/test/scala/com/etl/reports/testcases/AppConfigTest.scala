package com.etl.reports.testcases

import com.etl.utils.AppConfigReader
import org.scalatest.FunSuite

class AppConfigTest extends  FunSuite{

  test("app config test") {
    val config = AppConfigReader.getAppConfig(env = "prod")
    assert( config.purchases == "/data/purchases.csv")
    assert( config.events == "/data/events.csv")
    assert( config.campaignsRevenueOutputTable == "campaign_revenue")
    assert( config.channelEngagementOutputTable == "channel_engagement")
  }

  test("invalid config path") {
    assertThrows[NullPointerException](
      AppConfigReader.getAppConfig("invalid", "prod")
    )
  }

}

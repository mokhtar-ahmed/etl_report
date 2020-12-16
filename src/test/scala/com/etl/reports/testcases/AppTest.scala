package com.etl.reports.testcases

import com.etl.App
import com.etl.metadata.MetaData.{CampaignRevenue, ChannelEngagement}
import org.apache.spark.sql.SparkSession
import com.etl.utils.TestsConstants._

class AppTest extends BaseTest {

  test("App Main Test") {
    val ss:SparkSession = sparkSession
    import  ss.implicits._

    App.main(Array("test","20201216000000", "/config/application.properties", "click-stream-analytics"))

    val output1 = ss.sql(s"select * from ${config.channelEngagementOutputTable}").as[ChannelEngagement]
    val output2 = ss.sql(s"select * from ${config.campaignsRevenueOutputTable}").as[CampaignRevenue]

    output1.collect().toSeq should contain theSameElementsAs channelsEngagementExpected
    output2.collect().toSeq should contain theSameElementsAs campaignRevenuesExpected
  }
}

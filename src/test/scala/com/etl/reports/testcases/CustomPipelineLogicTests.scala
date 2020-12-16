package com.etl.reports.testcases

import com.etl.context.DataContextBuilder
import com.etl.reports.CustomPipelineLogic
import com.etl.utils.TestsConstants._
import org.apache.spark.sql._

class CustomPipelineLogicTests extends BaseTest {

  test("Event logic - Get session Test"){
    val ss:SparkSession = sparkSession
    val dataContext = DataContextBuilder.gather(ss, config)
    val sessions = CustomPipelineLogic.getSession(dataContext.events)
    log.info(s"records count = ${sessions.count()}")
    assert(sessions.count() == 6)
  }

  test("Event logic - Get Purchases Details Test"){
    val ss:SparkSession = sparkSession
    val dataContext = DataContextBuilder.gather(ss, config)
    val sessions = CustomPipelineLogic.getSession(dataContext.events)
    val purchases = CustomPipelineLogic.getPurchasesDetails(sessions, dataContext.purchases)
    log.info("purchase details dataset ...")
    purchases.collect().toSeq should contain theSameElementsAs purchaseDetailsExpected
  }

  test("Top 10 campaigns Test"){
    val ss:SparkSession = sparkSession
    import ss.implicits._
    val purchaseDetails = purchaseDetailsExpected.toDS
    val topCamps = CustomPipelineLogic.getTopCampaignsByRevenue(purchaseDetails)
    topCamps.collect() should contain theSameElementsAs campaignRevenuesExpected
  }

  test("Top channels by engagement Test") {
    val ss:SparkSession = sparkSession
    import ss.implicits._
    val purchaseDetails = purchaseDetailsExpected.toDS
    val topChannels = CustomPipelineLogic.getTopChannelsByEngagement(purchaseDetails)
    topChannels.collect().toSeq should contain theSameElementsAs channelsEngagementExpected
  }

}

package com.etl.reports.testcases

import com.etl.context.DataContextBuilder
import com.etl.reports.SQLPipelineLogic._
import com.etl.utils.TestsConstants._
import org.apache.spark.sql._

class SQLPipelineLogicTests extends BaseTest {

  test("Event logic - Get session Test"){
    val ss:SparkSession = sparkSession
    val dataContext = DataContextBuilder.gather(ss, config)
    val sessions = getSession(dataContext.events)
    sessions.show()
    println(s"records count = ${sessions.count()}")
    assert(sessions.count() == 20)
  }



  test("Event logic - Get Purchases Details Test"){
    val ss:SparkSession = sparkSession
    val dataContext = DataContextBuilder.gather(ss, config)
    val sessions = getSession(dataContext.events)
    val purchases = getPurchasesDetails(sessions, dataContext.purchases)
    println("purchase details dataset ...")
    purchases.show(false)
    purchases.collect().toSeq should contain theSameElementsAs purchaseDetailsExpected
  }


  test("Top 10 campaigns Test"){
    val ss:SparkSession = sparkSession
    import ss.implicits._
    val purchaseDetails = purchaseDetailsExpected.toDS
    val topCamps = getTopCampaignsByRevenue(purchaseDetails)
    topCamps.show()
    topCamps.collect() should contain theSameElementsAs campaignRevenuesExpected
  }


  test("Top channels by engagement Test") {
    val ss:SparkSession = sparkSession
    import ss.implicits._
    val purchaseDetails = purchaseDetailsExpected.toDS
    val topChannels = getTopChannelsByEngagement(purchaseDetails)
    topChannels.show()
    topChannels.collect().toSeq should contain theSameElementsAs channelsEngagementExpected
  }

}

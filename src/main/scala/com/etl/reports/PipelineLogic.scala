package com.etl.reports

import com.etl.metadata.MetaData._
import org.apache.spark.sql.Dataset

trait PipelineLogic {
  def getSession(events:Dataset[Event]):Dataset[Session]
  def getPurchasesDetails(sessions:Dataset[Session], purchases:Dataset[Purchase]):Dataset[PurchaseDetails]
  def getTopCampaignsByRevenue(purchasesDetails:Dataset[PurchaseDetails]): Dataset[CampaignRevenue]
  def getTopChannelsByEngagement(purchasesDetails:Dataset[PurchaseDetails]): Dataset[ChannelEngagement]
}

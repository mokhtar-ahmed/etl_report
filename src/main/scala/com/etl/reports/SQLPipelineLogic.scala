package com.etl.reports

import com.etl.metadata.MetaData._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SQLPipelineLogic extends PipelineLogic {

  override def getSession(events:Dataset[Event]):Dataset[Session] = {
    val ss:SparkSession = events.sparkSession
    import ss.implicits._

    events.filter('userId.isNotNull && 'eventId.isNotNull && 'eventType.isNotNull)
      .filter('eventType.isin(Seq("app_open", "purchase", "app_close") :_* ))
      .createOrReplaceTempView("events")

    ss.sql("""
         select sessionId,
                userId,
                case when eventType = 'app_open' then get_json_object(attributes, '$.campaign_id') else null end as campaignId,
                case when eventType = 'app_open' then get_json_object(attributes, '$.channel_id')  else null end as channelId,
                case when eventType = 'purchase' then get_json_object(attributes, '$.purchase_id') else null end as purchaseId
         from (
          select  t1.userId,
                  t1.eventId,
                  t1.eventTime,
                  t1.eventType,
                  t1.attributes,
                  CONCAT( t1.userId, CONCAT('_', SUM(t1.newSession) OVER (PARTITION BY t1.userId ORDER BY t1.eventTime))) AS sessionId
          from (
              select  userId,
                      eventId,
                      eventTime,
                      eventType,
                      attributes,
                      case when lag(eventType) over (partition by userId order by eventTime) == 'app_close' then 1 else 0 end  as newSession
              from events
          ) t1
          ) t2
      """).as[Session]
  }

  override  def getPurchasesDetails(sessions:Dataset[Session], purchases:Dataset[Purchase]):Dataset[PurchaseDetails] ={
    val ss:SparkSession = sessions.sparkSession
    import ss.implicits._

    val sessionPurchases = sessions.select('sessionId, 'userId ,'purchaseId)
      .filter('purchaseId.isNotNull)

    val sessionsMarketingChannels: Dataset[Row] = sessions.select('sessionId, 'campaignId, 'channelId)
      .filter('campaignId.isNotNull)
      .filter('channelId.isNotNull)
      .dropDuplicates()

    val purchasesDetails = purchases.join(sessionPurchases, Seq("purchaseId"), "inner")
                                 .join(sessionsMarketingChannels, Seq("sessionId"), "inner")
                                 .select('sessionId, 'userId , 'purchaseId, 'campaignId, 'channelId,
                                   'isConfirmed, 'billingCost,  'purchaseTime)

    purchasesDetails.as[PurchaseDetails]
  }

 override def getTopCampaignsByRevenue(purchasesDetails:Dataset[PurchaseDetails]):Dataset[CampaignRevenue] = {
    val ss:SparkSession = purchasesDetails.sparkSession
    import  ss.implicits._
    purchasesDetails.createOrReplaceTempView("purchasesDetails")
    ss.sql("""
        select campaignId,
               sum(billingCost) as totalRevenue
        from purchasesDetails
        where isConfirmed = true
        group by campaignId
        order by sum(billingCost) desc
      """)
      .as[CampaignRevenue]
  }

  override  def getTopChannelsByEngagement(purchasesDetails:Dataset[PurchaseDetails]):Dataset[ChannelEngagement] = {
    val ss:SparkSession = purchasesDetails.sparkSession
    import  ss.implicits._
    purchasesDetails.createOrReplaceTempView("purchasesDetails")
    ss.sql("""
        select channelId,
               count( distinct sessionId) as sessionsCount
        from purchasesDetails
        group by channelId
        order by count( distinct sessionId) desc
      """)
      .as[ChannelEngagement]
  }

}

package com.etl.reports

import com.etl.metadata.MetaData._
import org.apache.spark.sql.functions.{countDistinct, desc, sum}
import org.apache.spark.sql.{Dataset, SparkSession}

object CustomPipelineLogic extends PipelineLogic {

  override def getSession(events:Dataset[Event]):Dataset[Session] ={
    val ss:SparkSession = events.sparkSession
    import ss.implicits._

    val sessionEvents: Dataset[EventWithSessionId] = events.groupByKey(_.userId).mapGroups((x, xs) => {
      var id: Int = 0
      val sortedEvents = xs.toSeq.sortWith(_.eventTime before _.eventTime)
      val lagEvents: Seq[String] = Seq("") ++ sortedEvents.map(_.eventType)
       (sortedEvents zip lagEvents.takeRight(sortedEvents.size))
        .map(el => {
          val ev = EventWithSessionId(el._1.userId, el._1.eventId, el._1.eventTime, el._1.eventType,
            el._1.attributesMap, el._1.userId + "_" + id)
        if (el._2.equalsIgnoreCase("app_close")) id = id + 1
        ev
      })
    }).flatMap( x => x)

    val sessions: Dataset[Session] = sessionEvents.groupByKey(x => (x.sessionId, x.userId))
      .flatMapGroups((k, v) => {
          val evs = v.toSeq
          val openEvents = evs.filter(_.eventType.equalsIgnoreCase("app_open")).take(1)
          val campaign_id = openEvents(0).attributes.getOrElse("campaign_id", null)
          val channel_id = openEvents(0).attributes.getOrElse("channel_id", null)
          val purchasesEvents = evs.filter(_.eventType.equalsIgnoreCase("purchase"))
                                   .map(_.attributes.getOrElse("purchase_id", null))
                                    .filterNot( _ == null)

          if(purchasesEvents.size == 0 )
            Seq(Session(k._1, k._2, campaign_id, channel_id, null))
          else
            purchasesEvents.map( p_id => Session(k._1, k._2, campaign_id, channel_id, p_id))
      })

    sessions.filter(_.purchaseId != null)
  }

  override def getPurchasesDetails(sessions:Dataset[Session], purchases:Dataset[Purchase]):Dataset[PurchaseDetails] ={
    val ss:SparkSession = sessions.sparkSession
    import ss.implicits._

    val purchasesDetails = purchases.join(sessions, Seq("purchaseId"), "inner")
                                    .select('sessionId, 'userId , 'purchaseId, 'campaignId, 'channelId,
                                            'isConfirmed, 'billingCost,  'purchaseTime)

    purchasesDetails.as[PurchaseDetails]

  }

  override def getTopCampaignsByRevenue(purchasesDetails:Dataset[PurchaseDetails]):Dataset[CampaignRevenue] = {
    val ss:SparkSession = purchasesDetails.sparkSession
    import ss.implicits._
    purchasesDetails.filter(_.isConfirmed)
        .groupBy('campaignId)
        .agg(sum('billingCost) as "totalRevenue")
        .orderBy(desc("totalRevenue"))
        .as[CampaignRevenue]
  }

  override  def getTopChannelsByEngagement(purchasesDetails:Dataset[PurchaseDetails]):Dataset[ChannelEngagement] = {
    val ss:SparkSession = purchasesDetails.sparkSession
    import ss.implicits._
    purchasesDetails.groupBy('channelId)
        .agg(countDistinct('sessionId) as "sessionsCount")
        .orderBy(desc("sessionsCount"))
        .as[ChannelEngagement]
  }

}

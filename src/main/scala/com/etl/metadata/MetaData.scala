package com.etl.metadata

import java.sql.Timestamp

import scala.util.parsing.json.JSON

object MetaData {

  case class Session( sessionId:String,
                      userId:String,
                      campaignId:String,
                      channelId:String,
                      purchaseId:String
                    )

  case class Event( userId:String,
                    eventId:String,
                    eventTime:Timestamp,
                    eventType:String,
                    attributes:String
                  ){

    lazy val  attributesMap: Map[String, String] = {
      val default  = Map("" ->  "")
      if(attributes == null || attributes.isEmpty)  default
      else JSON.parseFull(attributes)  match {
        case Some(v) => v.asInstanceOf[Map[String, String]]
        case None => default
      }
    }
  }

  case class Purchase( purchaseId:String,
                       purchaseTime:Timestamp,
                       billingCost:Double,
                       isConfirmed:Boolean
                     )

  case class EventWithSessionId(  userId:String,
                                  eventId:String,
                                  eventTime:Timestamp,
                                  eventType:String,
                                  attributes: Map[String, String],
                                  sessionId: String
                               )

  case class PurchaseDetails( sessionId:String,
                              userId:String,
                              purchaseId:String,
                              campaignId:String,
                              channelId:String,
                              isConfirmed:Boolean,
                              billingCost:Double,
                              purchaseTime:Timestamp
                            )

  case class CampaignRevenue(
                            campaignId:String,
                            totalRevenue:Double
                            )

  case class ChannelEngagement(
                              channelId:String,
                              sessionsCount:Double
                            )

}

# Marketing Reports ETL

The target of this ETL to build to generate marketing reports.

The logic implemented into 2 classes CustomPipelineLogic and SQLPipelineLogic which implementing  PipelineLogic trait.

### Methods:

##### def getSession(events:Dataset[Event]):Dataset[Session]
getSession method is used to build the session based on the app events 
Input: events:Dataset[Event]  => userId,eventId,eventTime,eventType,attributes
Output: Dataset[Session] => sessionId,userId,campaignId,channelId,purchaseId

##### def getPurchasesDetails(sessions:Dataset[Session], purchases:Dataset[Purchase]):Dataset[PurchaseDetails]
getPurchasesDetails method is used to enrich the purchases action with the session details  
Input: sessions:Dataset[Session]  =>   sessionId, userId, campaignId, channelId, purchaseId
       purchases:Dataset[Purchase] =>  purchaseId, purchaseTime, billingCost, isConfirmed
        
Output: Dataset[PurchaseDetails] => sessionId,userId,purchaseId,campaignId,channelId,isConfirmed,billingCost,purchaseTime

##### def getTopCampaignsByRevenue(purchasesDetails:Dataset[PurchaseDetails]): Dataset[CampaignRevenue]
getTopCampaignsByRevenue to get the  campaigns revenues  
Input: Dataset[PurchaseDetails] => sessionId,userId,purchaseId,campaignId,channelId,isConfirmed,billingCost,purchaseTime
Output: campaignId, totalRevenue

#####  def getTopChannelsByEngagement(purchasesDetails:Dataset[PurchaseDetails]): Dataset[ChannelEngagement]
getTopChannelsByEngagement to get the channel engagement by session counts
Input: Dataset[PurchaseDetails] => sessionId,userId,purchaseId,campaignId,channelId,isConfirmed,billingCost,purchaseTime
Output  Dataset[ChannelEngagement] => channelId,sessionsCount



## How to run the Pipeline ?  

Run the main App Class and pass the following arguments
environment_name = test
batch_id = 20201216000000
config_file = application.properties
app_name = click-stream-app
 
 
spark-submit  --master yarn  \
              --driver-memory 8g  \
              --executor-memory 16g  \
              --executor-cores 2  \
              --class  com.etl.App \
              /path/to/jar/etl-marketing-reports.jar  test 20201216000000 application.properties click-stream-app


#### application.properties is used to specify the job/input/output configurations
##### events=/data/events.csv
##### purchases=/data/purchases.csv
##### campaignsRevenueOutputTable=campaign_revenue
##### channelEngagementOutputTable=channel_engagement
  
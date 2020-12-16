package com.etl.context
import com.etl.metadata.MetaData._
import com.etl.utils.AppConfigReader._
import org.apache.spark.sql.types.{BooleanType, DoubleType, TimestampType}
import org.apache.spark.sql.{Dataset, SparkSession}

object DataContextBuilder {

  case class DataContext(events:Dataset[Event], purchases:Dataset[Purchase])

  def gather(ss:SparkSession, modelConfig: AppConfig):DataContext = {
    import ss.implicits._

    val events: Dataset[Event] = ss.read
      .option("header", "true")
      .option("delimiter", "|")
      .csv(modelConfig.events)
      .select('userId, 'eventId, 'eventTime.cast(TimestampType), 'eventType, 'attributes)
      .as[Event]

    val purchases: Dataset[Purchase] = ss.read
      .option("header", "true")
      .option("delimiter", "|")
      .csv(modelConfig.purchases)
      .select('purchaseId, 'purchaseTime.cast(TimestampType), 'billingCost.cast(DoubleType), 'isConfirmed.cast(BooleanType))
      .as[Purchase]

    DataContext(events,  purchases)
  }

}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Analysis {

  def targetFeatures(df: DataFrame) {

    //count number of records
    val allRecords = df.count()
    println("Total no records: " + allRecords)

    //Average click-through rate for all records
    val avgCTR = df.select(avg("Click")).cache()
    avgCTR.show()
  }


  def distinctValues(df: DataFrame) {
    //TODO find faster way to do this
    val allCounts = df.select(
      countDistinct("BidID"), countDistinct("iPinYouID"), countDistinct("UserAgent"),
      countDistinct("IP"), countDistinct("Region"), countDistinct("City"),
      countDistinct("AdExchange"), countDistinct("Domain"), countDistinct("URL"),
      countDistinct("AnonymousURLID"), countDistinct("AdSlotID"), countDistinct("AdSlotWidth"),
      countDistinct("AdSlotHeight"), countDistinct("AdSlotVisibility"), countDistinct("AdSlotFormat"),
      countDistinct("AdSlotFloorPrice"), countDistinct("CreativeID"), countDistinct("BiddingPrice"),
      countDistinct("PayingPrice"), countDistinct("KeyPageURL"), countDistinct("AdvertiserID"),
      countDistinct("UserTags"), countDistinct("Click"), countDistinct("Year"),
      countDistinct("Month"), countDistinct("Day"), countDistinct("Hour"),
      countDistinct("TimeOfDay")
    ).cache()
    allCounts.show()

  }

}

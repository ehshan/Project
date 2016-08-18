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

  def creativeCTR(df: DataFrame){

    //average CTR for all Creative Id's
    val creative = df.groupBy("CreativeID").agg(avg("Click"))
    creative.show()

    //average CTR for all Ad widths
    val width = df.groupBy("AdSlotWidth").agg(avg("Click"))
    width.show()

    //average CTR for all Ad heights
    val height = df.groupBy("AdSlotHeight").agg(avg("Click"))
    height.show()

    //average CTR for all Ad format
    val format = df.groupBy("AdSlotFormat").agg(avg("Click"))
    format.show()

    //average CTR for all Ad page positions
    val visibility = df.groupBy("AdSlotVisibility").agg(avg("Click"))
    visibility.show()

  }

}

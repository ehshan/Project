
case class BidRequest (
                  val bidId: String,
                  val timestamp: String,
                  val iPinYouID: String,
                  val userAgent: String,
                  val ipAddress: String,
                  val region: String,
                  val city: String,
                  val adExchange: String,
                  val domain: String,
                  val url: String,
                  val anonymousURLID: String,
                  val adSlotID: String,
                  val adSlotWidth: String,
                  val adSlotHeight: String,
                  val adSlotVisibility: String,
                  val adSlotFormat: String,
                  val adSlotFloorPrice: String,
                  val creativeID: String,
                  val biddingPrice: Int,
                  val advertiserId: String,
                  val userTags: String
                )

object BidRequest{

  def apply():BidRequest={
    new BidRequest
  }
}

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

object BidRequest {

  def apply(request: String): BidRequest = {
    val feature = request.split("\t")

    new BidRequest(
      feature(0),
      feature(1),
      feature(2),
      feature(3),
      feature(4),
      feature(5),
      feature(6),
      feature(7),
      feature(8),
      feature(9),
      feature(10),
      feature(11),
      feature(12),
      feature(13),
      feature(14),
      feature(15),
      feature(16),
      feature(17),
      feature(18).toInt,
      feature(19),
      feature(20)
    )
  }
}
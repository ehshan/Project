sealed trait Algorithim {

  def getBidPrice(bidRequest: BidRequest): Int

}

class Bid extends Algorithim{

  //THE AVERAGE BIDDING PRICE FROM TRAINING DATA
  val baseBid = 88

  //THE AVERAGE CTR FROM TRAINING DATA
  val baseCTR = 8.237307135252534E-4

  val fixedBidPrice = 100


  /**
    * A Bid Response
    * @param bidRequest
    * @return
    */
  def getBidPrice(bidRequest: BidRequest):Int={

    val ctr = bidRequest.ctr

    val percent = ((ctr * 100.0f) / baseCTR)/100

    val price = (baseBid * percent).toInt

    price

  }

  def getAvgCTRPrice(bidRequest:BidRequest): Int ={

    //AN ARRAY OF ALL THE MEAN CTR VALUE FOR TARGET FEATURES
    val avgCTRs = BidConfig.getAllCTR(bidRequest)

    // THE CTR PARAMETER FOR THE LINEAR FORMULA
    val avgCTR = average(avgCTRs)

    0
  }

  /**
    * Returns a fixed bid amount
    * @return
    */
  def getConstantBid:Int={
    fixedBidPrice
  }

  /**
    * Return a Random Bid between max and min bid prices
    * @return
    */
  def getRandomBid:Int={
    val max = 300
    val min = 0
    val rnd = scala.util.Random

    min + rnd.nextInt( (max - min) + 1 )
  }

  /**
    * Helper method to get the mean value from an array of doubles
    * @param s
    * @return
    */
  def average(s: Seq[Double]): Double =
    s.foldLeft((0.0, 1)) ((acc, i) => (acc._1 + (i - acc._1) / acc._2, acc._2 + 1))._1


}
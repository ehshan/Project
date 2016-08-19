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

}
sealed trait Algorithim {

  def getBidPrice(bidRequest: BidRequest): Int

}

class Bid extends Algorithim{

  //THE AVERAGE BIDDING PRICE FROM TRAINING DATA
  val baseBid = 88

  //THE AVERAGE CTR FROM TRAINING DATA
  val baseCTR = 8.237307135252534E-4

  val fixedBidPrice = 100


  def getBidPrice(bidRequest: BidRequest):Int={

    0

  }


}
sealed trait Algorithim {

  def getBidPrice(bidRequest: BidRequest): Int

}

class Bid extends Algorithim{


  def getBidPrice(bidRequest: BidRequest):Int={

    0

  }


}
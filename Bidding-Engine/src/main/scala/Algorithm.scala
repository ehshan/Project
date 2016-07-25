
sealed trait Algorithim {

  def init()

  def getBidPrice(bidRequest: BidRequest): Int

}

class Bid extends Algorithim {


  def init(){

  }

  def getBidPrice(bidRequest: BidRequest): Int = {
    1
  }

}

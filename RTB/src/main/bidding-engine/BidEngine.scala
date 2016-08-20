import java.io.{FileInputStream, BufferedInputStream}

import scala.io.Source

/**
  * Created by Ehshan on 20/08/2016.
  */
object BidEngine {

  //SESSION-3
  val path = "D:\\_MSC_PROJECT\\sample-datasets\\i-pin-you-season-3\\leaderboard.test.data.20131021_28.txt"


  /**
    *
    * @return
    */
  def runSession(): Iterator[BidRequest] ={
    val logs =  Source.fromInputStream (new BufferedInputStream(new FileInputStream(path)))
    val request = (line: String) => handleRequest(line)
    val itt = logs.getLines().map(request)
    itt

  }

  /**
    * Returns the Bid Request Object for winning bids only
    *
    * @param s
    * @return
    */
  def handleRequest(s: String): BidRequest={
    val request = BidRequest(s)
    val marketPrice = request.payingPrice

    val bid = new Bid
    val ourBid = bid.getConstantBid

    val bidOption = if (marketPrice < ourBid) request else null

    bidOption
  }


}

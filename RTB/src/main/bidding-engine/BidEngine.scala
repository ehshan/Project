import java.io.{FileInputStream, BufferedInputStream}

import scala.io.Source

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

  /**
    * Method to convert the Bidding session results to a sequence
    *
    * @return
    */
  def convertToSeq(): Seq[BidRequest]={
    val result = runSession().toSeq
    result
  }

  /**
    * Evaluates each bidding session
    */
  def eval(): Unit ={

    val result = convertToSeq()


  }


}

import java.io.{FileInputStream, BufferedInputStream}

import scala.io.Source

object BidEngine {

  //THE LOCATION OF THE DATA CREATED BY RUNNING BidModel
  val path = "BidLogs"

  //SESSION-3 RAW DATA
//  val path = "ipinyou.contest.dataset\testing3rd\leaderboard.test.data.20131021_28.txt"


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

    val marketPrice = request.payingPrice //NORMAL MARKET
//    val marketPrice = request.biddingPrice //AGGRESSIVE MARKET


    val bid = new Bid
    val ourBid = bid.getBidPrice(request) // PREDICTED CTR STRATEGY
//    val ourBid = bid.getAvgCTRPrice(request) // AVERAGE CTR STRATEGY
//    val ourBid = bid.getConstantBid // FIXED BID STRATEGY
//    val ourBid = bid.getRandomBid // RANDOM BID STRATEGY

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

    //TOTAL NUMBER OF AUCTIONS
    val totalRequests = totalRecords()

    val wins = result.filter(x => x.isInstanceOf[BidRequest])

    val winCount = wins.length

    val spend = getSpend(wins)

    val clicks = getClicks(wins)

    val ctr = clicks.toFloat/winCount

    val cpc = spend.toFloat/clicks

    println("Bidding session ended, processing results...")
    println("-----------------------------------------------------------------------------------------------------------------------------")
    println("| Total Request: "+totalRequests+"| Total Wins: "+winCount+"| Total Spend: "+spend+" | Total Clicks: "+clicks+" | CTR: "+ctr+" | CPC "+cpc+" |")
    println("-----------------------------------------------------------------------------------------------------------------------------")

  }

  /**
    * Method to get the total number of bid requests
    * @return
    */
  def totalRecords():Int ={
    Source.fromFile(path).getLines().size
  }


  /**
    * Helper method to return the total bidding prices of a sequence of bid request
    *
    * @param bids
    * @return
    */
  def getSpend(bids: Seq[BidRequest]):Int ={

    //AGGRESSIVE
    //bids.foldLeft(0)((accum, bid) => accum + bid.biddingPrice)

    //NORMAL
    bids.foldLeft(0)((accum, bid) => accum + bid.payingPrice)
  }

  /**
    * Count the number of click won in bidding session
    * @param bids
    * @return
    */
  def getClicks(bids: Seq[BidRequest]):Int ={

    bids.foldLeft(0)((accum, bid) => accum + bid.clicks)
  }

  /**
    * Method to return the time of the bidding session
    * @param f
    * @return
    */
  def time(f: => Unit)={
    val s = System.currentTimeMillis
    f
    System.currentTimeMillis - s
  }



}

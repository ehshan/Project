/**
  * Created by Ehshan on 20/08/2016.
  */
object BidEngine {

  /**
    * Returns the Bid Request Object for winning bid only
    *
    * @param s
    * @return
    */
  def handleRequest(s: String): BidRequest={
    val request = BidRequest(s)

    val bid = new Bid

    request

  }


}

import org.scalatest.FunSuite


class BidTest extends FunSuite{

  //A SAMPLE BID REQUEST
  val line = "b8c557a16cdd9cea7fa61df79bdb392d\t" +
    "20131022071300438\t" +
    "1\t" +
    "DABNdj1osJ4\t" +
    "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; QQBrowser/7.4.14929.400)\t" +
    "4.216.169.*\t" +
    "216\t" +
    "233\t" +
    "2\t" +
    "5eaa6ffce5e827b0907e99679585db6b\t" +
    "48fb2f0ea154912f4371d76461bc061b\t" +
    "null\t" +
    "1528566466\t" +
    "468\t" +
    "60\t" +
    "OtherView\t" +
    "Na\t" +
    "5\t" +
    "7328\t" +
    "277\t" +
    "230\t" +
    "null\t" +
    "2259\t" +
    "10684,14273,10052,10133\t" +
    "0\t" +
    "0"


  test("Test Bid Class"){
    val bid = new Bid

    assert(bid != null)
  }

  test("Test Fixed Bid"){
    val bid = new Bid

    assert(bid.getConstantBid == 100)
  }

  test("Test mean CTR bidding"){
    val request = BidRequest(line)

    val bid = new Bid

    assert(bid.getAvgCTRPrice(request) == 179)
  }

}

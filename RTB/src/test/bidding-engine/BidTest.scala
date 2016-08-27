import org.scalatest.FunSuite


class BidTest extends FunSuite{

  test("Test Fixed Bid"){
    val bid = new Bid

    assert(bid.getConstantBid == 100)
  }

}

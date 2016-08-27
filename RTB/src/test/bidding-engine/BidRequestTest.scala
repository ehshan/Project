import org.scalatest.FunSuite


class BidRequestTest extends FunSuite{

  //A SAMPLE BID REQUEST
  val line =
    "88db9ffd2dc45e3f77fbffe53d01d5a5\t" +
      "20131020100000045\t" +
      "\t" +
      "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t" +
      "183.30.79.*\t" +
      "216\t" +
      "175\t" +
      "1\t" +
      "5777beaf611cab0f34afde6b9c3668c1\t" +
      "f618c3cc3c0b547025a30a13bbbf0b8a\t" +
      "null\t" +
      "mm_17936709_2306571_9246546\t" +
      "300\t" +
      "250\t" +
      "ThirdView\t" +
      "Fixed\t" +
      "0\t" +
      "7323\t" +
      "294\t" +
      "2259\t" +
      "null"


  test("Test BidRequest Constructor"){
    val request = BidRequest(line)

    assert(request.bidId == "88db9ffd2dc45e3f77fbffe53d01d5a5")
  }
}

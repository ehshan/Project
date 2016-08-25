import java.io.{FileInputStream, BufferedInputStream}

import scala.io.Source

object BidConfig {

  val features = Array("region","city","adExchange","adSlotWidth","adSlotHeight","adSlotVisibility",
    "adSlotFormat","creativeID")

  val path = "F:\\_MSC_PROJECT\\project-data\\bid-config\\"

  /**
    * Method to take a feature and get a map of all possible CTR values from config file and place to map
    *
    * @param feature
    * @return
    */
  def configFeature(feature: String): Map[String, Double] ={

    //DATABRICKS CVS WRITE IN PART FORMAT
    val p = path+feature+"\\part-00000"

    val data = Source.fromInputStream (new BufferedInputStream(new FileInputStream(p)))

    data.getLines.map {
      line => val Array(key,value) = line.split(',')
        val double = value.toFloat.toDouble
        key -> double }.toMap

  }

  /**
    * Method to get the CTR from a Bid Request of a particular feature value
    *
    * @param str
    * @param bidRequest
    * @param map
    * @return
    */
  def getCTR(str: String,bidRequest: BidRequest,map: Map[String,Double]): Double = {

    0
  }

}

import java.io.{FileInputStream, BufferedInputStream}

import scala.io.Source

object BidConfig {

  val features = Array("region","city","adExchange","adSlotWidth","adSlotHeight","adSlotVisibility",
    "adSlotFormat","creativeID")

  //path to config files
  val path = "configFiles"

  /**
    * Method to get all the ctr values for weighting
    *
    * @param bidRequest
    * @return
    */
  def getAllCTR (bidRequest: BidRequest):Array[Double] = {
    val ctr = (feature: String) => getCTR(feature, bidRequest,configFeature(feature))
    features.map(ctr)
  }

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

    //using reflection to get fields
    //we have to do Any here as bidRequest fields are of several types
    /**
      * Helper method using scala reflection to get the fields and values in BidRequest object
      *
      * @param obj
      * @return
      */
    def getFields(obj: Any): Map[String, Any] = {
      val fieldsAsPairs = for (field <- obj.getClass.getDeclaredFields) yield {
        field.setAccessible(true)
        (field.getName, field.get(obj))
      }
      Map(fieldsAsPairs :_*)
    }

    /**
      * Helper method to unpack object of BidRequest field -> value
      *
      * @param opt
      * @return
      */
    def unpackField(opt: Option[Any]) =
      opt match {
        case Some(s) => s
        case None => null
      }

    /**
      * Helper method to unpack double (ctr value) from map of features -> avgCTR
      *
      * @param opt
      * @return
      */
    def unpack(opt: Option[Double]) =
      opt match {
        case Some(s) => s
        case None => 0.0
      }

    //map contain all object fields and values
    val fields = getFields(bidRequest)

    //this is the raw filed value from BidRequest - same as if taken from object getter
    val feature = unpackField(fields.get(str)).toString

    val ctr = if (map.contains(feature)) unpack(map.get(feature)) else 0.0

    ctr
  }
}

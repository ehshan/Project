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

    0
  }

}

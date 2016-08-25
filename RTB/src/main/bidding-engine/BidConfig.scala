import java.io.{FileInputStream, BufferedInputStream}

import scala.io.Source

/**
  * Created by Ehshan on 24/08/2016.
  */
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


}

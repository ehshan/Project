import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataPrep {

  /**
    * Method to split probability vector from prediction to click and no-click columns
    *
    * @param df
    * @return
    */
  def splitProbability(df: DataFrame): DataFrame ={
    //SPLITS THE PROBABILITY TO CLICK/NO-CLICK PROBABILITIES
    val no: (Vector => (Double)) = (arg: Vector) => arg(0)
    val yes:(Vector => (Double)) = (arg: Vector) => arg(1)

    val noClickProb = udf(no)
    val clickProb = udf(yes)

    df/*withColumn("no-click-Probability", noClickProb(df("probability")))*/
      .withColumn("click-probability", clickProb(df("probability")))
      .drop("probability")
  }

  /**
    * Method to remove transformed features
    * @param df
    * @return
    */
  def removeTransformations(df:DataFrame): DataFrame ={
    df.drop("Month").drop("Day").drop("Click").drop("Hour").drop("TotalImpressions").drop("TotalAdViews")
      .drop("AdSlotWidth-vector").drop("AdSlotHeight-vector").drop("AdSlotVisibility-vector")
      .drop("AdSlotFormat-vector").drop("CreativeID-vector").drop("City-vector").drop("Month").drop("Region-vector")
      .drop("Hour-vector").drop("TotalAdViews-vector").drop("Month").drop("TotalImpressions-vector")
      .drop("features").drop("rawPrediction").drop("prediction")
  }


}

import java.io.PrintWriter

import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * @author Ehshan-Veerabangsa
  **/
object Store {

  /**
    * Splits a dataframe by advertiser, returns a map of DataFrames
    * @param df
    * @param sQLContext
    * @return
    */
  def splitByAdvertiser(df: DataFrame, sQLContext: SQLContext): Map[Any, DataFrame] = {
    //TODO instantiate single instances of SQL and Spark Contexts
    import sQLContext.implicits._

    //sequence of advertisers
    val advertisers = df.select("AdvertiserID").distinct.collect.flatMap(_.toSeq)

    //map containing all df's split by AdvertiserID - keys should be IDs
    val dfMap = advertisers.map(advertiser => advertiser -> df.where($"AdvertiserID" <=> advertiser)).toMap

    dfMap
  }

  /**
    * Writes the data-set to individual csv files - split by advertiser
    *
    * @param map
    */
  def saveByAdvertiser(map: Map[Any, DataFrame]){
    map.keys.foreach(i =>
      map(i).write.format("com.databricks.spark.csv").option("header", "true")
        .save(Data.path+"\\"+i)
    )
  }

  /**
    *
    * @param id
    * @param map
    * @return
    */
  def getFrameByAdvertiser(id: String, map: Map[Any,DataFrame]): DataFrame = {
    val option = map.get(id)

    /**
      * Helper method to unpack option
      *
      * @param opt
      * @return
      */
    def showFrame(opt: Option[DataFrame]) =
      opt match {
        case Some(s) => s
        case None => null
      }
    showFrame(option)
  }

  /**
    * Method to get single data-frame for algorithm  testing
    *
    * @param sc
    * @param sqlContext
    * @return
    */
  def getSingleFrame(sc: SparkContext, sqlContext: SQLContext): DataFrame ={
    val path = "D:\\_MSC_PROJECT\\written-data\\written-set\\3427.csv"

    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .load(path)
    df
  }

  /**
    * Method to write results to file
    *
    * @param metric
    */
  def saveModelResults(metric: BinaryClassificationMetrics){
    //GETTING VALUES

    val roc = metric.areaUnderROC()//AUROC
    val auPRC =  metric.areaUnderPR//AUPRC
    val precision = metric.precisionByThreshold// Precision by threshold
    val recall = metric.recallByThreshold// Recall by threshold

    //WRITING TO FILE

    val resultsPath = "Model-Results"
    val pw = new PrintWriter(resultsPath)

    pw.write("NEW RESULTS BATCH\r\n\r\n")
    pw.write("Area under ROC = " +roc+ "\r\n")
    pw.write("Area under precision-recall curve = " + auPRC+ "\r\n")

    //COLLECTED TO ARRAY, SERIALIZABLE
    val pcol = precision.collect()
    val rcol = recall.collect()

    pcol.foreach{ case (t, r) =>
      pw.write(s"Threshold: $t, Recall: $r"+ "\r\n")
    }

    rcol.foreach{ case (t, r) =>
      pw.write(s"Threshold: $t, Recall: $r"+ "\r\n")
    }

    pw.close()
  }

}

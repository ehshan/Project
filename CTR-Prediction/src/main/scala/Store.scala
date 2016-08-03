import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, DataFrame}

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

}

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._


import scala.io.Source

/**
  * @author Ehshan-Veerabangsa
  **/
object Data {

  //location of the data - change as appropriate
  val path = "D:\\_MSC_PROJECT\\datasets\\ipinyou-dataset\\ipinyou.contest.dataset"

  /**
    * Method to bulld a DataFrame with all log data for 2nd and 3rd seasons
    * @param sc
    * @param sqlContext
//    * @param path
    * @return
    */
  def build(sc: SparkContext, sqlContext: SQLContext): DataFrame = {

    //create the impression frame
    val imps = buildImpFrame(sc, sqlContext, buildSchema(".\\.\\.\\.\\schema"))

    //create the click frame
    val click = buildClickFrame(sc, sqlContext, buildSchema(".\\.\\.\\.\\schema"))

    //merge both frames
    val merge = mergeLogs(imps,click,sqlContext)

    //create the target variable
    val df= createTarget(merge)

    df
  }

  /**
    * Method to create a DataFrame schema from a text file
    * @param file
    * @return
    */
  def buildSchema(file: String): StructType = {
    var schemaString = ""

    for (line <- Source.fromFile(file).getLines) schemaString = schemaString + line + " "
    //create header for data-frame using column objects based on schema
    StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, nullable = true)))
  }

  /**
    * All impression logs to single DataFrame
    *
    * @param sc
    * @param sqlContext
    * @param schema
    * @return
    */
  def buildImpFrame(sc: SparkContext, sqlContext: SQLContext, schema: StructType): DataFrame = {

    //create data for all impression logs
    val impSecond = path + "\\training2nd\\imp*"
    val impThird = path + "\\training3rd\\imp*"

    val idf2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .schema(schema).option("delimiter", "\\t").load(impSecond)
    val idf3 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .schema(schema).option("delimiter", "\\t").load(impThird)

    val allImps = idf2.unionAll(idf3) //UNION ALL TO ADD ONE FRAME TO ANOTHER

    allImps
  }

  /**
    * All click logs to single DataFrame
    *
    * @param sc
    * @param sqlContext
    * @param schema
    * @return
    */
  def buildClickFrame(sc: SparkContext, sqlContext: SQLContext, schema: StructType): DataFrame = {

    //create data for all click logs
    val clickSecond = path + "\\training2nd\\clk*"
    val clickThird = path + "\\training3rd\\clk*"


    val cdf2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .schema(schema).option("delimiter", "\\t").load(clickSecond)
    val cdf3 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .schema(schema).option("delimiter", "\\t").load(clickThird)

    val allClicks = cdf2.unionAll(cdf3) //UNION ALL TO ADD ONE FRAME TO ANOTHER

    allClicks

  }

  /**
    * Methods to merge impression and click logs and remove duplicate records
    * @param i
    * @param c
    * @param sqlContext
    * @return
    */
  def mergeLogs(i: DataFrame, c: DataFrame, sqlContext: SQLContext): DataFrame={

    //casting timestamp to long
    val imps = castLong(i)
    val clicks = castLong(c)

    /**
      * Helper method to check whether two timestamps are within 5 minutes of each other
      * Impression falling within this range will represent the same bidding opportunity
      * @param impTime
      * @param clickTime
      * @return
      */
    def compareTime(impTime: Long, clickTime: Long): Boolean ={
      clickTime match {
        case x if impTime- 500000 until impTime + 500000 contains clickTime => true
        case _ => false
      }
    }
    val comp: ((Long, Long) => Boolean) = (arg1: Long, arg2:Long) => compareTime(arg1,arg2)

    val mergFun = udf(comp)

    sqlContext.udf.register("mergFun", comp)

    //temporary tables tp be evaluated by the SQL statement
    imps.registerTempTable("b")
    clicks.registerTempTable("a")

    //SQL statement to join tables on BidID and apply time checking function to each row
    val removeDuplicates: DataFrame = sqlContext.sql(
      """
        |SELECT b.*
        |FROM b
        |LEFT JOIN a ON a.BidID = b.BidID AND mergFun(a.Timestamp, b.Timestamp) <= true
        |WHERE a.LogType IS NULL
      """.stripMargin)

    //merging the impression frame with duplicate click removed with the click frame
    val result = removeDuplicates.unionAll(clicks)

    result
  }

  /**
    * Method to cast the Timestamp field to a long for analysis
    * @param df
    * @return
    */
  def castLong(df: DataFrame): DataFrame={

    /**
      * Helper method to cast a string to a long
      * @param x
      * @return
      */
    def stringToLong(x: String):Long={
      x.toLong
    }

    val cast: (String => Long) = (arg: String) => stringToLong(arg)

    val castFun = udf(cast)

    df.withColumn("TS", castFun(df("Timestamp")))
      .drop("Timestamp")
      .withColumnRenamed("TS", "Timestamp")

  }

  /**
    * Create a click target variable and appends it to a data-frame
    */
  def createTarget(df: DataFrame): DataFrame = {
    //function which maps LogType to click boolean
    val click: (String => Int) = (arg: String) => if (arg != "1") 1 else 0

    //make an sql function option (udf - user defined function)
    val clickFunc = udf(click)

    //creating new data-frame with appended column
    df.withColumn("Click", clickFunc(col("LogType"))).drop("LogType").cache()

  }

}
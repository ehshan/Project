import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._


import scala.io.Source

/**
  * @author Ehshan-Veerabangsa
  **/
object Data {

  //location of the data-change as appropriate
  val path = "D:\\_MSC_PROJECT\\datasets\\ipinyou-dataset\\ipinyou.contest.dataset"

  def main(args: Array[String]) {
    /*
     config Spark Engine
    */
    val conf = new SparkConf().setAppName("ctr-prediction").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    run(sc, sqlContext)
  }

  def run(sc: SparkContext, sqlContext: SQLContext) {
    val df = timeOfDay(transformTime(createTarget(buildDataFrame(sc, sqlContext, buildSchema(".\\.\\.\\.\\schema")))))
      .cache()
    //    df.show()
    //    targetFeatures(df) //COMMENT OUT FOR NOW
    val map = splitByAdvertiser(df, sqlContext)
  }

  def buildSchema(file: String): StructType = {
    var schemaString = ""

    for (line <- Source.fromFile(file).getLines) schemaString = schemaString + line + " "
    //create header for data-frame using column objects based on schema
    StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, nullable = true)))
  }

  def buildDataFrame(sc: SparkContext, sqlContext: SQLContext, schema: StructType): DataFrame = {
    /*
      create data-frame for all clicks
    */
    val clickSecond = path + "\\training2nd\\clk*"
    val clickThird = path + "\\training3rd\\clk*"

    val cdf2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .schema(schema).option("delimiter", "\\t").load(clickSecond).cache()
    val cdf3 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .schema(schema).option("delimiter", "\\t").load(clickThird).cache()

    val allClicks = cdf2.unionAll(cdf3) //UNION ALL TO ADD ONE FRAME TO ANOTHER

    /*
      create data-frame for all impressions
    */
    val impSecond = path + "\\training2nd\\imp*"
    val impThird = path + "\\training3rd\\imp*"

    val idf2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .schema(schema).option("delimiter", "\\t").load(impSecond).cache()
    val idf3 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .schema(schema).option("delimiter", "\\t").load(impThird).cache()

    val allImps = idf2.unionAll(idf3) //UNION ALL TO ADD ONE FRAME TO ANOTHER

    allImps.unionAll(allClicks) //joins all clicks and all imps
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

  def transformTime(df: DataFrame): DataFrame = {
    //transformation functions
    val getYear: (String => Int) = (arg: String) => convertTime(arg, Calendar.YEAR)
    val getMonth: (String => Int) = (arg: String) => convertTime(arg, Calendar.MONTH)
    val getDay: (String => Int) = (arg: String) => convertTime(arg, Calendar.DAY_OF_MONTH)
    val getHour: (String => Int) = (arg: String) => convertTime(arg, Calendar.HOUR_OF_DAY)

    val yearFunc = udf(getYear)
    val monthFunc = udf(getMonth)
    val dayFunc = udf(getDay)
    val hourFunc = udf(getHour)

    //adding new columns with new time features
    df.withColumn("Year", yearFunc(df("Timestamp"))).withColumn("Month", monthFunc(df("Timestamp")))
      .withColumn("Day", dayFunc(df("Timestamp"))).withColumn("Hour", hourFunc(df("Timestamp")))
      .drop("Timestamp").drop("Year").cache() //dropping original timestamp feature//years has only one value
  }

  /**
    * Helper method to convert timestamps
    */
  def convertTime(string: String, field: Int): Int = {
    // getting the date format from new object parsing
    val date = DateFormatter.formatter.get().parse(string)
    //getting a calendar instance
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.get(field)
  }

  def timeOfDay(df: DataFrame): DataFrame = {
    def time(td: Int): String = {
      td match {
        case x if 0 until 7 contains x => "midnight"
        case x if 7 until 12 contains x => "morning"
        case x if 12 until 16 contains x => "afternoon"
        case x if 16 until 20 contains x => "evening"
        case x if 20 until 23 contains x => "night"
        case x => null
      }
    }
    val tod: (Int => String) = (arg: Int) => time(arg)
    val timeFunc = udf(tod)
    df.withColumn("TimeOfDay", timeFunc(df("Hour"))).cache()
  }

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
    map.values.foreach(i =>
      map(i).write.format("com.databricks.spark.csv").option("header", "true")
        .save(path+"\\"+i)
    )
  }

  def targetFeatures(df: DataFrame) {

    //count number of records
    val allRecords = df.count()
    println("Total no records: " + allRecords)

    //Average click-through rate for all records
    val avgCTR = df.select(avg("Click")).cache()
    avgCTR.show()
  }


  def distinctValues(df: DataFrame) {
    //TODO find faster way to do this
    val allCounts = df.select(
      countDistinct("BidID"), countDistinct("iPinYouID"), countDistinct("UserAgent"),
      countDistinct("IP"), countDistinct("Region"), countDistinct("City"),
      countDistinct("AdExchange"), countDistinct("Domain"), countDistinct("URL"),
      countDistinct("AnonymousURLID"), countDistinct("AdSlotID"), countDistinct("AdSlotWidth"),
      countDistinct("AdSlotHeight"), countDistinct("AdSlotVisibility"), countDistinct("AdSlotFormat"),
      countDistinct("AdSlotFloorPrice"), countDistinct("CreativeID"), countDistinct("BiddingPrice"),
      countDistinct("PayingPrice"), countDistinct("KeyPageURL"), countDistinct("AdvertiserID"),
      countDistinct("UserTags"), countDistinct("Click"), countDistinct("Year"),
      countDistinct("Month"), countDistinct("Day"), countDistinct("Hour"),
      countDistinct("TimeOfDay")
    ).cache()
    allCounts.show()

  }
}
/**
  * Object to override the initial date format for a calendar object
  */
object DateFormatter {
  val formatter = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSS")
  }
}
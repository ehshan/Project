import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Object to extract second order data features
  *
  * @author Ehshan-Veerabangsa
  */
object Features {

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

  def viewsPerAdvertiser(df: DataFrame):DataFrame ={

    val adViews = df.groupBy("iPinYouID","AdvertiserID").count()
      .withColumnRenamed("count","TotalAdViews")
      .withColumnRenamed("AdvertiserID","AdID2")

    df.join(adViews,"iPinYouID")
      .drop("AdID2")
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

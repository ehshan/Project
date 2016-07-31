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
}
/**
  * Object to override the initial date format for a calendar object
  */
object DateFormatter {
  val formatter = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSS")
  }
}

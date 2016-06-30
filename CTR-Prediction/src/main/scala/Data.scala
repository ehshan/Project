import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._


import scala.io.Source

/**
  * @author Ehshan Veerabangsa
  **/
object Data {

  //location of the data-change as appropriate
  val path = "D:\\_MSC_PROJECT\\datasets\\ipinyou-dataset\\ipinyou.contest.dataset"

  def main (args: Array[String]){
    /*
     config Spark Engine
    */
    val conf = new SparkConf().setAppName("ctr-prediction").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    run(sc,sqlContext)
  }

  def run(sc: SparkContext, sqlContext: SQLContext){
    val df = createTarget(buildDataframe(sc, sqlContext, buildSchema(".\\.\\.\\.\\schema"))).cache()
    df.show()
    targetFeatures(df)
  }

  def buildSchema(file: String): StructType = {
    var schemaString = ""

    for (line <- Source.fromFile(file).getLines) schemaString = schemaString + line+" "
    //create header for data-frame using column objects based on schema
    StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, nullable = true)))
  }

  def buildDataframe(sc: SparkContext, sqlContext: SQLContext,schema: StructType): DataFrame ={
    /*
      create data-frame for all clicks
    */
    val clickSecond = path+"\\training2nd\\clk*"
    val clickThird = path+"\\training3rd\\clk*"

    val cdf2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(schema).option("delimiter", "\\t").load(clickSecond)
    val cdf3 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(schema).option("delimiter", "\\t").load(clickThird)

    val allClicks = cdf2.unionAll(cdf3)//UNION ALL TO ADD ONE FRAME TO ANOTHER

    /*
      create data-frame for all impressions
    */
    val impSecond = path+"\\training2nd\\imp*"
    val impThird = path+"\\training3rd\\imp*"

    val idf2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(schema).option("delimiter", "\\t").load(impSecond)
    val idf3 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(schema).option("delimiter", "\\t").load(impThird)

    val allImps = idf2.unionAll(idf3)//UNION ALL TO ADD ONE FRAME TO ANOTHER

    allImps.unionAll(allClicks)//joins all clicks and all imps
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
    df.withColumn("Click", clickFunc(col("LogType")))

  }

  def targetFeatures(df: DataFrame){
    /*
        count number of records
     */
    val allRecords = df.count()
    println("Total no records: "+allRecords)
    /*
        Average click-through rate for all records
    */
    val avgCTR = df.select(avg("Click"))
    avgCTR.show()
  }

  def transformTime(df: DataFrame): DataFrame = {
    df
  }

  /**
    * Helper method to convert timestamps
    */
  def convertTime(string: String, field: Int): Int = {
    // getting the date format from new object parsing
    val date = dateFormatter.formatter.get().parse(string)
    //getting a calendar instance
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.get(field)
  }
}
/**
  * Object to override the initial date format for a calendar object
  */
object dateFormatter {
  val formatter = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSS")
  }
}
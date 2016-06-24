import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.io.Source

/**
  * Created by Ehshan on 21/06/2016.
  */
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

  def run(sc: SparkContext, sqlContext: SQLContext): Unit ={
    val df = buildDataframe(sc, sqlContext, buildSchema(".\\.\\.\\.\\schema"))
    df.show()
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

    cdf2.unionAll(cdf3)//UNION ALL TO ADD ONE FRAME TO ANOTHER

  }

}
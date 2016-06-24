
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


  }

  def buildSchema(file: String): StructType = {
    var schemaString = ""

    for (line <- Source.fromFile(file).getLines) schemaString = schemaString + line+" "
    //create header for data-frame using column objects based on schema
    StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, nullable = true)))
  }

}

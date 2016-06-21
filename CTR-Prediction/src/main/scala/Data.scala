import java.io.File

import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.io.Source

/**
  * Created by Ehshan on 21/06/2016.
  */
object Data {

  //location of the data-change as appropriate
  val path = "E:\\_MSC_PROJECT\\datasets\\ipinyou-dataset\\ipinyou.contest.dataset"

  def main (args: Array[String]){}

  def buildSchema(file: File): StructType = {
    var schemaString = ""

    val filename = ".\\.\\.\\.\\schema"
    for (line <- Source.fromFile(filename).getLines) schemaString = schemaString + line+" "

    //create header for data-frame using column objects based on schema
    StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, nullable = true)))
  }

}

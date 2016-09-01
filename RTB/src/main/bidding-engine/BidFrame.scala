import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType

object BidFrame {

  val season2 = "ipinyou.contest.dataset\\testing2nd\\leaderboard.test.data.20130613_15.txt"

  val season3 = "ipinyou.contest.dataset\\testing3rd\\leaderboard.test.data.20131021_28.txt"

  /**
    * All impression logs to single DataFrame
    *
    * @param sc
    * @param sqlContext
    * @param schema
    * @return
    */
  def buildFrame(sc: SparkContext, sqlContext: SQLContext, schema: StructType): DataFrame = {

    val second = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .schema(schema).option("delimiter", "\\t").load(season2)

    val third = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .schema(schema).option("delimiter", "\\t").load(season3)

    second.unionAll(third)

  }
}

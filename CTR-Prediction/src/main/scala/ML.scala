import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

object ML {

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
    val df = Data.timeOfDay(Data.transformTime(Data.createTarget(Data.buildDataFrame(sc, sqlContext, Data.
      buildSchema(".\\.\\.\\.\\schema"))))).cache()
    //    df.show()
    //    targetFeatures(df) //COMMENT OUT FOR NOW
//    val map = Data.splitByAdvertiser(df, sqlContext)//COMMENT OUT FOR NOW
  }

  /**
    * Helper method to cast click column from a string to a double
    *
    * @param df
    * @return
    */
  def castTypes(df: DataFrame): DataFrame={
    val castDf = df
      .withColumn("clickTmp", df("Click").cast(DoubleType))
      .drop("Click")
      .withColumnRenamed("clickTmp", "Click")
    castDf
  }

  /**
    * Applying logistic regression algorithm using a single feature
    *
    * @param df
    */
  def singleFeature(df: DataFrame){
    val weights = Array(0.8, 0.2)
    val seed = 11L
    val df = castTypes(df)
  }

}

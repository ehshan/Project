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
    * Applying logistic regression algorithm using a single feature
    * @param df
    */
  def singleFeature(df: DataFrame){

  }

}

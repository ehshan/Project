import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object BidModel {

  //PATH WHERE TRAINED DATA WILL BE WRITTEN TO. CHANGE AS DESIRED
  val path = ""

  def prepData()={
    val conf = new SparkConf().setAppName("bid-model").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //LOAD FRAME
    val df = BidFrame.buildFrame(sc,sqlContext,Data.buildSchema("schema"))

    //SECOND ORDER FEATURES
    val so = Features.addSecondOrder(df)

    //ADD CLICK LABEL
    val withTarget = DataPrep.createTarget(so)

    //TRANSFORM MODEL
    val model = ML.bidModel(withTarget)

    //TRANSFORM DATA BACK TO ORIGINAL FORMAT
    val clean = DataPrep.removeTransformations(model)

    //SPLIT PROBABILITY VECTOR TO GET CTR PROBABILITY
    val dataForEngine = DataPrep.splitProbability(clean)

    dataForEngine.coalesce(1).write.format("com.databricks.spark.csv").option("header", "false")
      .option("delimiter", "\t").save(path)

  }



}

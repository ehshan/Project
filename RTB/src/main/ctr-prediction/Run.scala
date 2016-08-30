import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * @author Ehshan-Veerabangsa
  **/
object Run {

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("ctr-prediction").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //PRE-PROCESS DATA
    val df = Data.build(sc,sqlContext)

    //FEATURE EXTRACTION
    val so = Features.addSecondOrder(df)

    //FEATURE TRANSFORMATION
    val binarySet = ModelData.binaryFeatures(so)

    //MODEL TRAINING AND EVALUATION
    val model = ML.lrModel(binarySet)

  }
}

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

    val df = Store.getSingleFrame(sc, sqlContext)

    ML.singleFeature(ML.castTypes(df))
    ML.multiFeatures(ML.castTypes(df))
  }
}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite


class NumericSetTest extends FunSuite{

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test-numeric-set").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //PRE-PROCESS DATA
    val df = Data.build(sc, sqlContext)

    //EXTRACT AND ADD SECOND ORDER FEATURES
    val transformed = Features.addSecondOrder(df)

    //CREATE INDEXED DATA
    val numeric = ModelData.numericalFeatures(transformed)

    //CHECK DATA
    numeric.show()

    //CHECK DATA FORMAT
    numeric.printSchema()

  }

}

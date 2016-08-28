import org.scalatest.FunSuite
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


class TransformTest extends FunSuite{

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test-transform-data").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //PRE-PROCESS DATA
    val df = Data.build(sc, sqlContext)

    //EXTRACT AND ADD SECOND ORDER FEATURES
    val transformed = Features.addSecondOrder(df)

    //CHECK DATA
    transformed.show()

    //CHECK DATA FORMAT
    transformed.printSchema()

  }
}

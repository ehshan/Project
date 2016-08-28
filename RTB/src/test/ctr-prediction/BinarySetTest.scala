import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite


class BinarySetTest extends FunSuite{

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test-binary-set").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //PRE-PROCESS DATA
    val df = Data.build(sc, sqlContext)

    //EXTRACT AND ADD SECOND ORDER FEATURES
    val transformed = Features.addSecondOrder(df)

    //CREATE BINARY DATA
    val binary = ModelData.binaryFeatures(transformed)

    //CHECK DATA
    binary.show()

    //CHECK DATA FORMAT
    binary.printSchema()

  }

}

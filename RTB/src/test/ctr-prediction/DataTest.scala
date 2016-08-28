import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite


class DataTest extends FunSuite{

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test-data-frame").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //PRE-PROCESS DATA
    val df = Data.build(sc, sqlContext)

    //CHECK DATA
    df.show()

    //CHECK DATA FORMAT
    df.printSchema()

  }

}

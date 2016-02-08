import scala.math._
/**
  * Created by ehshan on 08/02/2016.
  */
object HigherOrderFunctions {

  def main(args : Array[String]){

    val logTenFunction = log10 _

    println(logTenFunction(1000))

    List(1000.0, 10000.0).map(logTenFunction).foreach(println)

    List(1,2,3,4,5).map((x : Int) => x * 50).foreach(println)

    List(1,2,3,4,5).filter(_ % 2 == 0).foreach(println)

    def timesThree(num : Int) = num * 3

    def timesFour(num : Int) = num * 4

    //passing function to a function
    def multIt (func : (Int) => Double, num : Int) ={
      func(num)
    }
    printf("4 x 100 = %.1f\n", multIt(timesFour, 100))

    //clojure
    val divisorVal = 5;
    val divisorFive = (num : Double) => num / divisorVal
    println("5 / 5 = " + divisorFive(5.0))

  }

}

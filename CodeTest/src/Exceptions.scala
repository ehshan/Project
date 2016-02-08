/**
  * Created by ehshan on 08/02/2016.
  */
object Exceptions {

  def main(args: Array[String]): Unit ={

    def divideNums(num1 : Int, num2 : Int ) = try{
      num1 / num2
    }catch{
      case ex : java.lang.ArithmeticException => "Can't divide by 0"
    }finally{
      //clean up
    }

    println("4 / 0 = " + divideNums(3,0))

  }// end of main

}// end of object

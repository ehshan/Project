/**
  * Created by ehshan on 04/02/2016.
  */
object Functions {

  def main(args:Array[String]){

//    FUNCTION STRUCTURE
//    def functionName(param1: dataType, param2: dataType): returnType ={
//      function body
//      return valueToReturn
//
//    }

    //basic function
    def getSum(num1: Int = 1, num2: Int = 1): Int ={
      num1 + num2
    }
    println("10 + 1 = "+getSum(10,1))

    println("Using named args in function will give same result -> "+getSum(num1 = 10, num2 = 1))

    //sum
    def getTotal(args: Int*): Int = {
      var total : Int = 0
      for (num <- args){
        total += num
      }
     total //returning this
    }
    println("Total of 4,8,12 is "+getTotal(4,8,12))

    //recursion
    def factorial(num : BigInt) : BigInt ={
      if (num <= 1)
        1
      else
        num*factorial(num -1)
    }
    println("Factorial of 11 is "+factorial(4))

//    1st : num = 4 * factorial(3) = 4*6 = 24
//    2nd : num = 3 * factorial(2) = 3*2 = 6
//    3rd : num - 2 * factorial(1) = 2*1 = 2

    //function that do not return values -> know as procedure
    def sayHi(): Unit ={
      println("Hi, how's it going")
    }
    sayHi()


  }
}

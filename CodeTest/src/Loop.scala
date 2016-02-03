/**
  * Created by ehshan on 03/02/2016.
  */

//simple scala class for testing
object Loop {


  //simple incrementer
  def main(args: Array[String]): Unit = {
    var i = 0
    while(i <= 10){
      println(i)
      i += 1
    }

    //print alphabet
    val alphabet  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for(i <- 0 until alphabet.length){
      println(alphabet(i))
    }

    //print even numbers
    val evenList = for {i <- 1 to 20
      if (i % 2) == 0
    } yield i
    for (i <- evenList) println(i)

    //multi dimensional print
    for (i <- 0 to 4; j <- 1 to 5) {
      println("Loop " + alphabet(i) + ": Number " + j)
    }

    //function printing
    def printPrimes(): Unit ={
      val primeList = List (1,2,3,5,7,11)
      for (i <- primeList){
        if(i == 11){
          return
        }

        if (i != 1){
          print(i)
        }
      }
    }
    printPrimes()
  }
}

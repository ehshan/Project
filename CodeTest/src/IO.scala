//import scala.io.StdIn{readLine, readInt}
import scala.math
import scala.collection.mutable.ArrayBuffer
import java.io.PrintWriter
import scala.io.Source
/**
  * Created by ehshan on 04/02/2016.
  */

//simple scala class for testing IO
object IO {

  def main(args: Array[String]){

    var numberGusss = 0

    do{
      print("Guess a number ")
      numberGusss = readLine.toInt

    }while(numberGusss != 15)

    printf("You guessed the secret number %d\n" ,15)

    val name = "Ehshan"
    val age = 29
    val height = 188.5

    println(s"Hello! I'm $name")

    println(f"I'm  ${age + 1} and am $height%.2fcm tall")
  }

}

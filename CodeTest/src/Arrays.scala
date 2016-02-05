import scala.collection.mutable.ArrayBuffer

/**
  * Created by ehshan on 05/02/2016.
  */
object Arrays {
  def main (args : Array[String]){
    //use array with the number item is fixed
    //use array buffer when there is a variable number of items

    //create array
    val numberList = new Array[Int](20)

    //print numberList
    for (i <- 0 to (numberList.length -1)){
      numberList(i) = i
      print(numberList(i)+ " ")
    }
    println()

    //applying function to list (x2)
    val numberListTimesTwo = for(num <-numberList) yield  2* num
    numberListTimesTwo.foreach(print)
    println()

    //applying function to list (div4)
    val numberListDividedFour = for(num <- numberList if num % 4 == 0) yield num
    numberListDividedFour.foreach(print)
    println()

    //library function
    println("Sum of Number List : "+ numberList.sum)
    println("Min value of Number List : "+ numberList.min)
    println("Max value of Number List : "+ numberList.max)

    //library sorts
    val sortedNumList = numberList.sortWith(_>_)
    print(sortedNumList.deep.mkString(", "))
    println()

    //string array
    val friends = Array("Tom", "Dick", "Harry")
    //change array value
    friends(0)= "Bob"
    print("I have "+friends.length+" friends. They are: ")
    for(i <- friends.indices)
      print(friends(i)+" ")

    //create an array buffer
    val newFriends = ArrayBuffer[String]()
    //adding friends
    newFriends.insert(0, "John")
    newFriends +="Mandy"

    //adding multiple friends with array
    newFriends ++= Array("Jimmy","Jimbo")
    newFriends.insert(1, "Mike","Micky","Sue","Suzy")

    //remove a fiend (sad-face)
    newFriends.remove(1,2)

    println()
    print("I have "+newFriends.length+ " new friends. They are ")

    //loop through all my friends
    var friend : String = ""
    for(friend <- newFriends)
      print(friend+ " ")

    //multidimensional arrya
    var multiTable = Array.ofDim[Int](10,10)
    for (i <- 0 to 9)
      for (j <- 0 to 9)
        multiTable(i)(j) = i*j

    //print multidimensional
    println("Multiplication Table")
    for (i <- 0 to 9)
      for (j <- 0 to 9)
       printf("%d x %d = %d\n", i, j, multiTable(i)(j))


  }
}

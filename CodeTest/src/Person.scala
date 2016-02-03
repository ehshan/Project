/**
  * Created by ehshan on 02/02/2016.
  */
//simple scala class for testing
class Person(
     val firstName: String,
     val lastName: String,
     val age: Int,
     val sex: String,
     val occupation: String
     ){

     def fullName: String = firstName + " " + lastName

     def isAdult(age : Int): Boolean ={
       if (age > 17) {
         return true
       }
       false
     }
}




/**
  * Created by ehshan on 02/02/2016.
  */
class Person(
     val firstName: String,
     val lastName: String,
     val age: Int,
     val adult: Boolean,
     val sex: String,
     val occupation: String
     ){

     def fullName: String = firstName + " " + lastName

     def adult(age : Int): Boolean ={
       if (age < 17) {
         return true
       }
       false
     }
}




/**
  * Created by ehshan on 07/02/2016.
  */
object Classes {

  def main(args: Array[String]): Unit ={

    val rover = new Animal()
    rover.setName("Rover")
    rover.setSound("Woof")
    printf("%s says %s\n", rover.getName(), rover.getSound())

    val whiskers = new Animal("Whiskers", "Hi, I'm a cat")
    println(s"${whiskers.getName()} with id ${whiskers.id} says ${whiskers.getSound()}")
    //with toString
    println(whiskers.toString())

    val spike = new Dog("Spike","Hello, I'm a dog","Grrrr, I like being a dog")
    println(spike.toString())

    //create a wolf
    val whiteFang = new Wolf("White Fang")
    whiteFang.moveSpeed = 40
    println(whiteFang.move)

  }//end of main

  //default constructor
  class Animal(var name: String, var sound: String){

    //protect variables
    this.setName(name)

    val id = Animal.newIdNumber

    //scala getters
    def getName():String = name
    def getSound():String = sound

    //scale setters
    def setName(name: String){
      //regex to check for any type of decimal
      if(!(name.matches(".*\\d+.*")))
        this.name = name
      else
        this.name = "No Name"
    }

    def setSound(sound: String){
      this.sound = sound
    }

    //other constructor -> if no sound passes
    def this(name:String){
      this("No Name", "No Sound")
      this.setSound(name)
    }

    //constructor if no params passes
    def this(){
      this("No Name","No Sound")
    }

    override def toString() : String = {
      "%s with the ID %d says %s".format(this.name, this.id, this.sound)
    }

  }

  //conpaanion object where one can find static class methods and functions
  object Animal{
    private var idNumber = 0
    private def newIdNumber = {idNumber +=1; idNumber}
  }

  class Dog(name:String, sound: String, growl: String) extends Animal(name, sound){

    //constructor with no growl
    def this(name : String, sound : String){
      this("No Name",sound,"No Growl")
      this.setName(name)
    }

    //constructor with no sound & no growl
    def this(name: String){
      this("No Name", "No Sound","No Growl")
      this.setName(name)
    }

    //constructor with no params
    def this(){
      this("No Name", "No Sound","No Growl")
    }

    override def toString(): String ={
      "%s with the ID %d says %s or %s".format(this.name, this.id, this.sound, this.growl)
    }
  }

  //abstract class
  abstract class Mammal(val name: String){

    //abstract var has no value assigned to it
    var moveSpeed : Double

   // abstract method with no return type
    def move : String
  }

  class Wolf(name: String) extends Mammal(name){

    var moveSpeed = 35.0

    def move = "The wolf %s runs at %.2f mph".format(this.name, this.moveSpeed)
  }


}//end of class

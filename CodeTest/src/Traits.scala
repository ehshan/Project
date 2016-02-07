/**
  * Created by ehshan on 07/02/2016.
  */
object Traits {

  def main(args: Array[String]): Unit ={

    val superman = new Superhero("Superman")
    println(superman.fly)
    println(superman.hitByBullet)
    println(superman.ricochet(2500))

    val batman = new Superhero("Batman")
    println(batman.ricochet(10000))
    println(batman.partnership("Robin"))

  }//end of main

  trait Flyable{
    def fly : String
  }

  trait BulletProof{
    def hitByBullet : String

    def ricochet(startSpeed : Double) : String ={
      "The bullet richocets at a speed of %.1f ft/sec".format(startSpeed * .75)
    }
  }

  trait SideKick{

    def sideKick (sideName : String) : String = {
      "has %s as a sidekick".format(sideName)
    }

    def partnership(sideName : String): String
  }

  class Superhero(val name: String) extends Flyable with BulletProof with SideKick{


    def fly = "%s flys through the pain".format(this.name)

    def hitByBullet = "The bullet bounces off %s".format(this.name)

    def partnership(sideName : String) = "%s ".format(this.name)+this.sideKick(sideName)
  }


}// end of class

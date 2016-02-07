/**
  * Created by ehshan on 07/02/2016.
  */
object Tuples {

  def main(args: Array[String]){

    //tuples are normally immutable

    // id 103 owes £10.25
    var tupleTest =(103, "Leonid Brezhnev", 10.25)

    //                            second tuple, third tuple
    printf("%s owes me £%.2f\n", tupleTest._2, tupleTest._3)

    //to print all value in all tuple -> use product iterator with for each
    tupleTest.productIterator.foreach(i => println(i) )

    //to convert a tuple to string
    println(tupleTest.toString())
  }

}

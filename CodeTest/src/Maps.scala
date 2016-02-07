/**
  * Created by ehshan on 07/02/2016.
  */
object Maps {

  def main(args: Array[String]){

    // an immutable map
    //first value in map is the key
    val employees = Map("Manager" -> "Bob Smith",
      "Secretary" -> "Sue Brown")

    //check if map has a manager and print
    if (employees.contains("Manager"))
      printf("The Manager is %s\n", employees("Manager"))


    //create a mutable map -> add collection.mutable
    val customers = collection.mutable.Map(100 -> "Paul Smith",
      101 -> "Sally Smith")

    //checing first customer
    printf("The first customer is %s\n", customers(100))

    //changing map value
    customers(100) = "Karl Marx"

    //adding a value
    customers(102) = "Josef Starlin"

    //printing all map keys with values
    for((key,value) <- customers)
      printf("%d : %s\n", key,value)
  }

}

/**
  * Created by ehshan on 04/02/2016.
  */
object Strings {

  def main (args: Array[String]){

    var sentence = "I am awesome"

    println("3rd Index: "+sentence(3))
    println("String Lenght: "+sentence.length())
    println(sentence.concat(" and a born winner"))
    println("Are the strings equal? "+"I am not awesome".equals(sentence))
    println("Awesome starts at index "+sentence.indexOf("awesome"))

    val sentenceArray = sentence.toArray
    print("Sentence from an array: ")
    for (i <- sentenceArray){
      print(i)
    }

  }

}

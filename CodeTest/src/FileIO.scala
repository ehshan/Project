import java.io.PrintWriter
import scala.io.Source
/**
  * Created by ehshan on 08/02/2016.
  */
object FileIO {

  def main(args : Array[String]): Unit ={

    val writer = new PrintWriter("test.txt")
    writer.write("Hello, this is a file\nWith loads of cool text")
    writer.close()

    //open connection to file
    val textFromFile = Source.fromFile("test.txt", "UTF-8")

    //create an iterator
    val lineIterator = textFromFile.getLines

    //iterate through all file line and print
    for(line <- lineIterator)
      println(line)

    //close connection to file
    textFromFile.close()
  }
}

package org.example


import org.apache.spark.sql.SparkSession
import org.example.chapter3.{Exercise1, Exercise2}
import org.example.chapter4.Exercise3
import org.example.chapter5.Exercise4
import org.example.chapter6.Exercise5
object Main {
  def main(args: Array[String]): Unit = {
    println("Select exercise:")
    for (x <- 1 to 5) {
      println(s"$x. Exercise $x")
    }

    val exerciseNumber = scala.io.StdIn.readInt()


    exerciseNumber match {
      case 1 => Exercise1.solve()
      case 2 => Exercise2.solve()
      case 3 => Exercise3.solve()
      case 4 => Exercise4.solve()
      case 5 => Exercise5.solve()
      case _ => println("Selection not valid")
    }


  }
}
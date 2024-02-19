package org.example.chapter5

import org.example.sparkSessionProvider.SparkSessionProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Exercise4 {
  /*
  Will not work as Hive issues with Spark SQL remain
   */
  def solve(): Unit = {
    val spark = SparkSessionProvider.createSparkSession("Test")
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    val cubed = (s: Long) => {
      s * s * s
    }
    // Register UDF
    spark.udf.register("cubed", cubed)
    // Create temporary view
    spark.range(1, 9).createOrReplaceTempView("udf_test")

    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

    spark.sql("""CREATE TABLE people (name STRING, age int)""")
    spark.sql("""INSERT INTO people VALUES ("Michael", NULL)""")
    spark.sql("""INSERT INTO people VALUES ("Andy", 30)""")
    spark.sql("""INSERT INTO people VALUES ("Samantha", 19)""")

    spark.sql("""SHOW TABLES""")
    spark.sql("""SELECT * FROM people WHERE age < 20""")
    spark.sql("""SELECT name FROM people WHERE age IS NULL;""")
  }

}

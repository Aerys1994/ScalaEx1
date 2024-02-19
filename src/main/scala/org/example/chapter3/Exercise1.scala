package org.example.chapter3
import org.example.sparkSessionProvider.SparkSessionProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row




object Exercise1 {
  /*
  Ejercicio sobre un df personalizado para probar las funcionalidades básicas de
  las API de Spark.
  Capítulo 3
  */
  def solve(): Unit = {

    val spark = SparkSessionProvider.createSparkSession("Test")
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val jsonFile = "src/main/resources/json/blogs.json"
    // Define our schema programmatically
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))

    val blogsDF = spark.read.schema(schema).json(jsonFile)

    blogsDF.show(false)
    println(blogsDF.printSchema)
    println(blogsDF.schema)
    println(blogsDF.columns)
    blogsDF.select(col("Hits") * 2).show(2)
    blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()
    blogsDF
      .withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id"))))
      .select(col("AuthorsId"))
      .show(4)
    blogsDF.select(col("Hits")).show(2)
    blogsDF.sort(col("Id").desc).show()

    val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
      Array("twitter", "LinkedIn"))
    blogRow(1)

    val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
    val authorsDF = rows.toDF("Author", "State")
    authorsDF.show()

  }
}

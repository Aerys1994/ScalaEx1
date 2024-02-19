package org.example.chapter4
import org.apache.hadoop.fs.Options.HandleOpt.path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.ByteType.json
import org.example.sparkSessionProvider.SparkSessionProvider


object Exercise3 {
  def solve(): Unit = {
    /*
    Ejercicio para ver el uso de SQL en Spark.
    Capítulo 4
     */


    val spark = SparkSessionProvider.createSparkSession("Test")

    spark.sparkContext.setLogLevel("ERROR")

    val csvFile="src/main/resources/departuredelays.csv"

    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)

    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    df.createOrReplaceTempView("us_delay_flights_tbl")

    spark.sql(
      """SELECT distance, origin, destination
    FROM us_delay_flights_tbl WHERE distance > 1000
    ORDER BY distance DESC""").show(10)

    spark.sql(
      """SELECT date, delay, origin, destination
    FROM us_delay_flights_tbl
    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
    ORDER by delay DESC""").show(10)

    spark.sql(
      """SELECT delay, origin, destination,
    CASE
    WHEN delay > 360 THEN 'Very Long Delays'
    WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
    WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
    WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
    WHEN delay = 0 THEN 'No Delays'
    ELSE 'Early'
    END AS Flight_Delays
    FROM us_delay_flights_tbl
    ORDER BY origin, delay DESC""").show(10)

    spark.catalog.listDatabases().show()
    spark.catalog.listTables().show()
    spark.catalog.listColumns("us_delay_flights_tbl").show()

    //Managed
    //spark.sql("""CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT,
      //distance INT, origin STRING, destination STRING)""").show()
    //Unmanaged

    spark.sql(
      """CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
    distance INT, origin STRING, destination STRING)
    USING csv OPTIONS (PATH
    'src/main/resources/departuredelays.csv')""")


    val file =
      "src/main/resources/parquet/2010-summary.parquet"
    val df_parquet = spark.read.format("parquet").load(file)
    println("check table")
    spark.sql("SELECT * FROM us_delay_flights_tbl").show()

    val jsonFile =
      "src/main/resources/json/*"
    val dfJson = spark.read.option("mode", "PERMISSIVE").json(jsonFile)
    val dfWithoutCorruptRecords = removeCorruptRecords(dfJson)
    dfWithoutCorruptRecords.show()

    spark.sql(
      """CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
        USING json
        OPTIONS (
          path "src/main/resources/json/*"
        )"""
    ).show()

    df.write.format("json")
      .mode("overwrite")
      .option("compression", "snappy")
      .save("src/main/resources/json/df_json")

    val fileCsv = "src/main/resources/csv/*"
    val schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"
    val dfCsv: DataFrame = spark.read.format("csv")
      .schema(schema)
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("nullValue", "")
      .load(fileCsv)

    dfCsv.show()

    spark.sql(
      """CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
        |USING csv
        |OPTIONS (
        |path "src/main/resources/csv/*",
        |header "true",
        |inferSchema "true",
        |mode "FAILFAST"
        |)""".stripMargin
    ).show()

    dfCsv.write.format("csv").mode("overwrite").save("src/main/resources/csv/output")





  }

  def removeCorruptRecords(df: DataFrame): DataFrame = {
    if (df.columns.contains("_corrupt_record")) {
      df.filter(col("_corrupt_record").isNull)
    } else {
      df
    }
  }




}

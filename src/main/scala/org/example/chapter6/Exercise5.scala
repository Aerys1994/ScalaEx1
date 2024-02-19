package org.example.chapter6

import org.example.sparkSessionProvider.SparkSessionProvider
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.functions._



object Exercise5 {

  case class Usage(uid: Int, uname: String, usage: Int)

  case class UsageCost(uid: Int, uname: String, usage: Int, cost: Double)

  def computeCostUsage(usage: Int): Double = {
    if (usage > 750) usage * 0.15 else usage * 0.50
  }

  def computeUserCostUsage(u: Usage): UsageCost = {
    val cost = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
    UsageCost(u.uid, u.uname, u.usage, cost)
  }

  def solve(): Unit = {
    val spark = SparkSessionProvider.createSparkSession("Test")
    spark.sparkContext.setLogLevel("ERROR")

    implicit val usageEncoder: Encoder[Usage] = Encoders.product[Usage]

    // Generate data
    val r = new scala.util.Random(42)
    val data = for (i <- 0 to 1000)
      yield Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000))

    // Create Dataset
    val dsUsage = spark.createDataset(data)
    dsUsage.show(10)

    val dsUsageFilter = dsUsage
      .filter(d => d.usage > 900)
      .orderBy(desc("usage"))
    dsUsageFilter.show()

    val dsUsage2 = dsUsage.map(u => {
      if (u.usage > 750) Usage(u.uid, u.uname, u.usage * 0.15.toInt)
      else Usage(u.uid, u.uname, u.usage * 0.50.toInt)
    })

    dsUsage2.show(5, false)



    import spark.implicits._
    val dsUsage3 = dsUsage.map(u => computeCostUsage(u.usage))
    dsUsage3.show(5, false)

    val dsUsageCost = dsUsage.map(u => computeUserCostUsage(u))
    dsUsageCost.show(5)

  }
}

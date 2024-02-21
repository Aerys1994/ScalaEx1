package org.example.chapter7
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SaveMode


object Exercise6 {

  def printConfigs(session: SparkSession) = {
    // Get conf
    val mconf = session.conf.getAll
    // Print them
    for (k <- mconf.keySet) {
      println(s"${k} -> ${mconf(k)}\n")
    }
  }
  def solve(): Unit = {
    val spark = SparkSession.builder
      .config("spark.sql.shuffle.partitions", 5)
      .config("spark.executor.memory", "2g")
      .master("local[*]")
      .appName("SparkConfig")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    printConfigs(spark)
    spark.conf.set("spark.sql.shuffle.partitions",
      spark.sparkContext.defaultParallelism)
    println(" ****** Setting Shuffle Partitions to Default Parallelism")
    printConfigs(spark)

    spark.sql("SET -v").select("key", "value").show(5, false)

    println(spark.conf.get("spark.sql.shuffle.partitions"))
    spark.conf.set("spark.sql.shuffle.partitions", 5)
    println(spark.conf.get("spark.sql.shuffle.partitions"))


    val df = spark.range(1 * 10000000).toDF("id").withColumn("square", col("id") * col("id"))
    println(df.cache())
    println(df.count())

    println(df.persist(StorageLevel.DISK_ONLY))
    println(df.count())

    df.createOrReplaceTempView("dfTable")
    spark.sql("CACHE TABLE dfTable")
    spark.sql("SELECT count(*) FROM dfTable").show()


    import spark.implicits._
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    var states = scala.collection.mutable.Map[Int, String]()
    var items = scala.collection.mutable.Map[Int, String]()
    val rnd = new scala.util.Random(42)

    states += (0 -> "AZ", 1 -> "CO", 2 -> "CA", 3 -> "TX", 4 -> "NY", 5 -> "MI")
    items += (0 -> "SKU-0", 1 -> "SKU-1", 2 -> "SKU-2", 3 -> "SKU-3", 4 -> "SKU-4",
      5 -> "SKU-5")

    val usersDF = (0 to 1000000).map(id => (id, s"user_${id}",
        s"user_${id}@databricks.com", states(rnd.nextInt(5))))
      .toDF("uid", "login", "email", "user_state")
    val ordersDF = (0 to 1000000)
      .map(r => (r, r, rnd.nextInt(10000), 10 * r * 0.2d,
        states(rnd.nextInt(5)), items(rnd.nextInt(5))))
      .toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")
    val usersOrdersDF = ordersDF.join(usersDF, col("users_id") === col("uid"))
    usersOrdersDF.show(false)


    usersDF.orderBy(asc("uid"))
      .write.format("parquet")
      .bucketBy(8, "uid")
      .mode("overwrite")
      .saveAsTable("UsersTbl")
    ordersDF.orderBy(asc("users_id"))
      .write.format("parquet")
      .bucketBy(8, "users_id")
      .mode("overwrite")
      .saveAsTable("OrdersTbl")
    // Cache the tables
    spark.sql("CACHE TABLE UsersTbl")
    spark.sql("CACHE TABLE OrdersTbl")
    // Read them back in
    val usersBucketDF = spark.table("UsersTbl")
    val ordersBucketDF = spark.table("OrdersTbl")
    // Do the join and show the results
    val joinUsersOrdersBucketDF = ordersBucketDF
      .join(usersBucketDF, $"users_id" === $"uid")
    joinUsersOrdersBucketDF.show(false)

  }
}

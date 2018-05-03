package net.cqwu.apachespark

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object SQLDataSourceExample {
  case class Person(name: String,age: Long)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    //runBasicParquetExample(spark)
   // runParquetSchemaMergingExample(spark)
  //  runJsonDatasetExample(spark)
    runJdbcDatasetExample(spark)
     spark.stop()
  }

  private def runBasicDataSourceExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val userDF = spark.read.load("D:\\spark\\examples\\src\\main\\resources\\users.parquet")
    userDF.select("name","favorite_color").write.mode(SaveMode.Overwrite).save("namesAndFavColors.parquet")
    val peopleDF = spark.read.format("json").load("D:\\spark\\examples\\src\\main\\resources\\people.json")
    peopleDF.select("name","age").write.format("parquet").mode(SaveMode.Overwrite).save("namesAndAges.parquet")
    peopleDF.show()
    val peopleDFCSV = spark.read.format("csv")
      .option("sep",";")
      .option("inferSchema","true")
      .option("header","true")
      .load("D:\\spark\\examples\\src\\main\\resources\\people.csv")
    peopleDFCSV.show()
    peopleDFCSV.select("name","age")show()
    val sqlDF = spark.sql("SELECT * FROM parquet.`D:\\spark\\examples\\src\\main\\resources\\users.parquet`")
    println("++++++++++++++++++++++++++++++++++")
    peopleDF.write.bucketBy(10,"name").sortBy("age").saveAsTable("people_bucketed")
    spark.sql("SELECT * FROM people_bucketed").show()
    //sqlDF.show()
    userDF.write.partitionBy("favorite_color").format("parquet").mode(SaveMode.Overwrite).save("namesPartByColor.parquet")
    userDF.write.partitionBy("favorite_color").bucketBy(10,"name").saveAsTable("users_partitioned_bucketed")
    spark.sql("drop table if exists people_bucketed")
    spark.sql("drop table if exists users_partitioned_bucketed")
  }
    private def runBasicParquetExample(spark: SparkSession): Unit = {
          import spark.implicits._
      val peopleDF = spark.read.json("D:\\spark\\examples\\src\\main\\resources\\people.json")
      peopleDF.write.mode(SaveMode.Overwrite).parquet("peopleDF.parquet")
      val parquetDF = spark.read.parquet("peopleDF.parquet")
      parquetDF.createOrReplaceTempView("parquetDF")
      val res = spark.sql("SELECT name FROM parquetDF")
      res.map(row => row.getString(0)).show()
     // parquetDF.show()
    }
  private def runParquetSchemaMergingExample(spark: SparkSession): Unit = {
      import spark.implicits._
    //implicit val schema =
    var squaresDF = spark.createDataFrame((1 to 6).map(i => (i,i * i)))
    squaresDF = spark.sparkContext.parallelize((1 to 6).map(x => (x,x * x))).toDF("value","square")
    squaresDF.write.mode(SaveMode.Overwrite).parquet("data/test_table/key=1")
    val cubesDF = spark.sparkContext.makeRDD((6 to 10).map(x => (x, x * x))).toDF("value","cube")
    cubesDF.write.mode(SaveMode.Overwrite).parquet("data/test_table/key=2")
    val mergedDF = spark.read.option("mergeSchema","true").parquet("data/test_table")
    mergedDF.show()
    mergedDF.printSchema()
  }

  private def runJsonDatasetExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val peopleDF = spark.read.json("D:\\spark\\examples\\src\\main\\resources\\people.json")
    peopleDF.printSchema()
    peopleDF.createOrReplaceTempView("people")
    val teenagerNamesDF = spark.sql("SELECT name FROM people")
    teenagerNamesDF.show()
    //将String字符串转为String List
    val otherPeopleDataset = spark.createDataset("""{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    //将dataset转为df
    val otherPeople = spark.read.json(otherPeopleDataset)
    otherPeople.show()
  }
  private def runJdbcDatasetExample(spark: SparkSession): Unit = {
     val jdbcDF = spark.read.format("jdbc")
      .option("url","jdbc:mysql://192.168.1.205/test")
      .option("dbtable","test")
      .option("user","root")
      .option("passwd","hadoop")
      .load()
    //jdbcDF.show()
     val connectionProperties = new Properties()
    connectionProperties.put("user","root")
    connectionProperties.put("password","hadoop")
    val jdbcDF2 = spark.read.jdbc("jdbc://mysql:192.168.1.205:/test","test.test",connectionProperties)
    jdbcDF2.show()
    jdbcDF.write.format("jdbc")
      .option("url","jdbc://mysql:192.168.1.205/test")
      .option("dbtable","test.test")
      .option("user","root")
      .option("password","hadoop")
      .save()
  }
}

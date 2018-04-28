package net.cqwu.apachespark

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, Row, SparkSession}


object DataSetTest {
  case class People(name: String,age: Long)
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
        .builder()
        .appName(this.getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate()
      //runBasicDataFrameExample(spark)
      //runDatasetCreationExample(spark)
      //runInferSchemaExample(spark)
      runProgrammaticSchemaExample(spark)
    }

  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    val df = spark.read.json("D:\\spark\\examples\\src\\main\\resources\\people.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    import spark.implicits._
    df.select($"name",$"age" + 1).show()
    df.select($"age" > 21).show()
    df.groupBy("age").count().show()
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    sqlDF.show()
    df.createOrReplaceGlobalTempView("gpeople")
    val sqlDFG = spark.sql("select * from gpeople")
    sqlDFG.show
    val otherSessionSql = spark.newSession().sql("select * from gpeople")
    otherSessionSql.show()
  }

  private def runDatasetCreationExample(spark: SparkSession): Unit = {
      import spark.implicits._
    val caseClassDS = Seq(People("zp",23),People("andy",32)).toDS()
    caseClassDS.show()
    val primitiveDS = Seq((1,2,3),(4,5,6)).toDS()
    primitiveDS.show()
    val res1 = primitiveDS.map(_._1 + 1)
    res1.show()
    import spark.implicits._
    val peopleDS = spark.read.json("D:\\spark\\examples\\src\\main\\resources\\people.json").as[People]
    peopleDS.show()
  }

  //通过反射:将RDD转为DataFrom/DataSet
  private def runInferSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val input = spark.sparkContext.textFile("D:\\spark\\examples\\src\\main\\resources\\people.txt")
      .map{p =>
        val strs = p.split(",")
        People(strs(0),strs(1).trim.toLong)
      }.cache()
    val peopleDS = input.toDS()
    peopleDS.show()
    val resDS1 = peopleDS.map(_.name)
    println("-------------")
    resDS1.show()
    val peopleDF = input.toDF()
    println("-------------")
    val resDF1 =  peopleDF.map(_.getString(0))
    resDF1.show()
    peopleDF.createOrReplaceTempView("peopleDF")
    val teenagersDF = spark.sql("SELECT name,age FROM peopleDF WHERE age BETWEEN 13 AND 19")
    teenagersDF.show()
    teenagersDF.map(x => x(0).toString).show()
    teenagersDF.map(_.getAs[String]("name")).show()
    println("=======")
    //spark 仅支持原始数据类型和case class的序列化，其他需要自定义序列化类
    implicit val mapEncoder = Encoders.kryo[Map[String,Any]]
    teenagersDF.map(x => x.getValuesMap[Any](List("name", "age"))).collect()
  }

  private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    val  schemaString = "name age"
    val fields = schemaString.split(" ").map(StructField(_,StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD = spark.sparkContext.textFile("D:\\spark\\examples\\src\\main\\resources\\people.txt")
      .map(_.split(",")).map(attrs => Row(attrs(0),attrs(1).trim))
    import spark.implicits._
    val peopleDF = spark.createDataFrame(rowRDD,schema)
    peopleDF.createOrReplaceTempView("people")
    spark.sql("SELECT name FROM people").show()
  }
}

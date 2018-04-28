package net.cqwu.apachespark

import org.apache.spark.sql.{SaveMode, SparkSession}

case class Record(key: Int,value: String)
object RDDRelation {
    def main(args: Array[String]): Unit = {
         val spark = SparkSession
        .builder()
        .appName(this.getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate()
      import spark.implicits._
      val df = spark.createDataFrame((0 to 10).map(i => Record(i,s"var_$i")))
      df.show()
      df.createOrReplaceTempView("record")
      val res01 = spark.sql("SELECT count(*) from record").collect()
      println("====")
      println(res01.head.get(0))
      df.map(row => row.getString(1)).show()
      println(df.rdd.map(row => row.get(1)).collect().mkString(","))
      println("=================")
      df.where($"key" > 1).orderBy($"value".asc).select("key").show()
      df.write.mode(SaveMode.Overwrite).parquet("pair.parquet")
      val parquetFile = spark.read.parquet("pair.parquet")
      println("+++++++++++++++++++++++++++++++")
      parquetFile.where($"key" > 5).select($"value".as("a")).show()
      //parquetFile.show()
      parquetFile.createOrReplaceTempView("parquetFile")
      spark.sql("SELECT * FROM parquetFile").show()
      spark.stop()
    }
}

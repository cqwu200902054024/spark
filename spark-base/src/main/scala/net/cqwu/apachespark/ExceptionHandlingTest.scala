package net.cqwu.apachespark

import org.apache.spark.sql.SparkSession

object ExceptionHandlingTest {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
        .builder()
        .appName(this.getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate()
      val input = spark.sparkContext.parallelize(0 until 100).foreach{i =>
          if(i > 100) {
            throw new Exception("error")
          }
      }
      spark.stop
    }
}

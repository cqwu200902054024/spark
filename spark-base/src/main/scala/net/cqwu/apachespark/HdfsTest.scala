package net.cqwu.apachespark

import org.apache.spark.sql.SparkSession

object HdfsTest {
    def main(args: Array[String]): Unit = {
       val spark = SparkSession
        .builder()
        .appName(this.getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate()
      val input = spark.read.text("D:\\spark\\data\\graphx").rdd
      val lineLength = input.map[Int](x => x.get(0).toString.length).cache()
      for(x <- lineLength) {
        println(x)
      }
      spark.stop()
    }
}

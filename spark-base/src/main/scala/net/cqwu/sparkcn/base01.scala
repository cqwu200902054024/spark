package net.cqwu.sparkcn

import org.apache.spark.sql.SparkSession

object base01 {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("base01").master("local[1]").getOrCreate()
      import spark.implicits._
     // var x = 4
      val input = spark.createDataset(List(1,2,3,34,4))
   /*   val res = input.foreach((y) => {
        x = x + y
      })*/
     // print(x)
      val pairs = input.map((_,1))
      print(pairs.printSchema())
      val res = pairs.groupByKey(_._1)
      println(res.count.show())
      spark.stop()
    }
}
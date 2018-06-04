package net.cqwu.sougousearch

import org.apache.spark.sql.SparkSession

/**
  * 搜狗日志分析
  */
object SougouLog {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
        .master("local[*]")
        .appName(this.getClass.getSimpleName)
        .getOrCreate()
      val input = spark.sparkContext.textFile("D:\\data").map(_.split("\\s")).filter(_.length == 6).cache()
      //1724264
      println(input.count())
      val res1 = input.filter(_(3).toInt == 1).filter(_(4).toInt == 1).map(_(1))
      //res1.foreach(println)
      import spark.implicits._
      val res2 = input.map{x =>
        (x(1),1)
      }.reduceByKey(_ + _).map{x =>
        (x._2,x._1)
      }.sortByKey(ascending = false).map{x =>
        (x._2,x._1)
      }
      res2.filter(_._2 > 10).foreach{x =>
        println(x._1 + "\t" + x._2)
      }

      res2.saveAsTextFile("D:\\data1")
/*      res2.foreach{x =>
        println(x._1 + "\t" + x._2)
      }*/
      spark.stop()
    }
}

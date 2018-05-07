package net.cqwu.apachespark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HdfsWordCount {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
        val ssc = new StreamingContext(sparkConf,Seconds(2))
        val inputStream = ssc.textFileStream("D:\\spark\\examples\\src\\main\\resources\\test.txt")
        val res = inputStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).print()
      println(res)
      ssc.start()
      ssc.awaitTermination()
    }
}

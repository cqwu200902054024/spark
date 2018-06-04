package net.cqwu.apachespark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {
    def main(args: Array[String]): Unit = {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
      val ssc = new StreamingContext(sparkConf,Seconds(2))
      val inputFromNetWork = ssc.socketTextStream("localhost",22,StorageLevel.MEMORY_AND_DISK_SER)
      val res = inputFromNetWork.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
      ///
      val pairs = inputFromNetWork.flatMap(_.split(" ")).map((_,1))
      //  val rs = pairs.updateStateByKey()
      res.print()
      res.count()
      ssc.start()
      ssc.awaitTermination()
    }//
}

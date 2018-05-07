package net.cqwu.apachespark.streaming

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object QueueStream {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(sparkConf,Seconds(2))
    val queueRDDs = new mutable.Queue[RDD[Int]]()
    val inputQueue = ssc.queueStream(queueRDDs)
    val res = inputQueue.map(x => (x % 10,1)).reduceByKey(_ + _)
    res.print()
    ssc.start()
    for(i <- 0 to 100) {
      queueRDDs.synchronized {
        queueRDDs += ssc.sparkContext.makeRDD(0 to 1000,10)
      }
      Thread.sleep(1000)
    }
    ssc.awaitTermination()
  }
}

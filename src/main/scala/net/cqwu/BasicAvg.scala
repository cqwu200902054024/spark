package net.cqwu

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BasicAvg01 {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"   
    }
    val sparkConf = new SparkConf().setMaster(master).setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)
    val input = sc.parallelize[Int](List(1, 2, 3, 4, 5, 6), 3)
    println(input.collect())
    sc.stop()
  }
}
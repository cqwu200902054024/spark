package net.cqwu.sparkcn

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object GroupByTest {
    def main(args: Array[String]): Unit = {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
      val sc = new SparkContext(sparkConf)
      var numMappers = 100
      var numKVPairs = 100000
      var valSize = 1000
      var numReducers = 36
      val pairs1 = sc.parallelize(0 until numMappers,numMappers).flatMap{ p =>
           val ranGen = new Random
           var arr1 = new Array[(Int,Array[Byte])](numKVPairs)
           for(i <- 0 until numKVPairs) {
             val byteArr = new Array[Byte](valSize)
             //随机填充字节数组
             ranGen.nextBytes(byteArr)
             arr1(i) = (ranGen.nextInt(Int.MaxValue),byteArr)
           }
        arr1
      }.cache()
      pairs1.count()
      println(pairs1.groupByKey(numReducers).count())
      sc.stop()
    }
}

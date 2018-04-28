package net.cqwu.apachespark

import scala.collection.JavaConverters._
import org.apache.spark.util.Utils

object DriverSubmissionTest {
    def main(args: Array[String]): Unit= {
      val numSecondsToSleep = args(0).toInt
      val env = System.getenv()
      val properties = env.asScala
      env.asScala.filter{
        case (k,_) => k.contains("SPARK_TEST")
      }.foreach{
        println
      }
      properties.filter{
        case (k,_) => k.toString.contains("spark.test")
      }.foreach(println)
    }
}

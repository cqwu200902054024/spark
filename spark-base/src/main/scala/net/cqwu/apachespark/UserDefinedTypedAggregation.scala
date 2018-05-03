package net.cqwu.apachespark

import org.apache.spark.sql.{Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

object UserDefinedTypedAggregation {
  case class Employee(name: String,salary: Long)
  case class Average(var sum: Long,var count: Long)
  object MyAverage extends Aggregator[Employee,Average,Double] {
     def zero: Average = Average(0L,0L)

     def reduce(buffer: Average, employee: Employee): Average = {
       buffer.sum += employee.salary
       buffer.count += 1
       buffer
     }

    def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

    def bufferEncoder: Encoder[Average] = Encoders.product

    def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val ds = spark.read.json("D:\\spark\\examples\\src\\main\\resources\\employees.json").as[Employee]
    val averageSalary = MyAverage.toColumn
    val result = ds.select(averageSalary)
    result.show()
    spark.stop()
  }
}

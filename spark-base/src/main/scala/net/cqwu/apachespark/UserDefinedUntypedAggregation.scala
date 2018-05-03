package net.cqwu.apachespark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object UserDefinedUntypedAggregation {

  //自定义一个聚合器（无类型）
  object MyAverage extends UserDefinedAggregateFunction {
    //聚合函数输入参数数据类型
    def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

    //中间数据类型
    def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)

    //返回数据类型
    def dataType: DataType = DoubleType

    // Whether this function always returns the same output on the identical input
    def deterministic: Boolean = true

    // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
    // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
    // the opportunity to update its values. Note that arrays and maps inside the buffer are still
    // immutable.
    //一个可以更新的row
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    // Updates the given aggregation buffer `buffer` with new input data from `input`
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer2.getLong(1) + buffer2.getLong(1)
    }

    // Calculates the final result
    def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble / buffer.getLong(1)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val df = spark.read.json("D:\\spark\\examples\\src\\main\\resources\\employees.json")
    spark.udf.register("myAverage",MyAverage)
    df.createOrReplaceTempView("em")
    val res = spark.sql("SELECT myAverage(salary) as average_salary FROM em")
    res.show()
    spark.stop()
  }
}

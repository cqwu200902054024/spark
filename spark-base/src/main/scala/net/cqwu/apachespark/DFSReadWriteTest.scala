package net.cqwu.apachespark

import java.io.File

import org.apache.spark.sql.SparkSession

import scala.io.Source._

object DFSReadWriteTest {
    private var localFilePath: File = new File(".")
    private var dfsDirPath: String = ""
    private val NPARAMS = 2
    private def readFile(filename: String): List[String] = {
      val lineIter: Iterator[String] = fromFile(filename).getLines()
      val lineList: List[String] = lineIter.toList
      lineList
    }

  def runLocalWordCount(fileContents: List[String]): Int = {
    fileContents.flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .groupBy(w => w)
      .mapValues(_.size)
      .values
      .sum
  }

  def main(args: Array[String]): Unit = {
    val fileContents = readFile(localFilePath.toString)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    //将数据写入hdfs
    val dfsFileName = s"dfsDirPath/dfs_read_write_test"
    val fileRDD = spark.sparkContext.parallelize[String](fileContents)
    fileRDD.saveAsTextFile(dfsFileName)

    val readFileRDD = spark.sparkContext.textFile(dfsFileName)
    val res = readFileRDD
      .flatMap[String](_.split(" "))
      .flatMap[String](_.split("\t"))
      .filter(_.nonEmpty)
      .map[(String,Int)]((_,1))
      .countByKey()
      .values
      .sum
    spark.stop()
  }
}

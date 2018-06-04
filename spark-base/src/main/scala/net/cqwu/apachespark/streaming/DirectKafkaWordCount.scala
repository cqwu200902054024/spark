package net.cqwu.apachespark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object DirectKafkaWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext,Seconds(10))
    val topicSet = Set("test1","test2")
    val kafkaParams = Map[String,String]("metadata.broker.list" -> "brokers")
    val inputMessage = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicSet)
    ssc.checkpoint("hdfs://")
  }
}

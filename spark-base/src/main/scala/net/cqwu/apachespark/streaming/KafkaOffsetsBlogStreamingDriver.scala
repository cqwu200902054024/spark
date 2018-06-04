package net.cqwu.apachespark.streaming

import kafka.utils.ZkUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaOffsetsBlogStreamingDriver {
  def main(args: Array[String]): Unit = {
    if(args.length < 6) {
      System.err.println("Usage: KafkaDirectStreamTest <batch-duration-in-seconds> <kafka-bootstrap-servers> \" +\n\"<kafka-topics> <kafka-consumer-group-id> <hbase-table-name> <kafka-zookeeper-quorum>");
      System.exit(1);
    }

    val batchDuration = args(0)
    val bootstrapServers = args(1).toString()
    val topicSet = args(2).split(",").toSet
    val consumerGroupID = args(3)
    val hbaseTablename = args(4)
    val zkQuorum = args(5)
    val zkKafkaRootDir = "kafka"
    val zkSessionTimeOut = 10000
    val zkConnectionTimeOut = 10000
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(batchDuration.toInt))
    val topics = topicSet.toArray
    val topic = topics(0)
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> consumerGroupID,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    def  processMessage(message: ConsumerRecord[String,String]): ConsumerRecord[String,String] = {
      message
    }

    /**
      * save offsets into HBase
      */
    def  saveOffsets(TOPIC_NAME: String,GROUP_ID: String,offsetRanges: Array[OffsetRange],hbaseTableName: String,batchTime: org.apache.spark.streaming.Time) = {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.addResource("")
      val conn = ConnectionFactory.createConnection(hbaseConf)
      val table = conn.getTable(TableName.valueOf(hbaseTableName))
      val rowKey = TOPIC_NAME + ":" + GROUP_ID + ":" + String.valueOf(batchTime.milliseconds)
      val put = new Put(rowKey.getBytes())
      for(offset <- offsetRanges) {
       put.addColumn(Bytes.toBytes("offsets"),Bytes.toBytes(offset.partition.toString),
         Bytes.toBytes(offset.untilOffset.toString)
       )
      }
      table.put(put)
      conn.close()
    }

    /*
   Returns last committed offsets for all the partitions of a given topic from HBase in following cases.
     - CASE 1: SparkStreaming job is started for the first time. This function gets the number of topic partitions from
       Zookeeper and for each partition returns the last committed offset as 0
     - CASE 2: SparkStreaming is restarted and there are no changes to the number of partitions in a topic. Last
       committed offsets for each topic-partition is returned as is from HBase.
     - CASE 3: SparkStreaming is restarted and the number of partitions in a topic increased. For old partitions, last
       committed offsets for each topic-partition is returned as is from HBase as is. For newly added partitions,
       function returns last committed offsets as 0
    */

    def getLastCommitedOffsets(TOPIC_NAME: String,GROUP_ID: String,hbaseTableName: String,zkQuorum: String,
                               zkRootDir: String,sessionTimeOut: Int,connectionTimeOut: Int): Map[TopicPartition,Long] = {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.addResource("")
      val zkUrl = zkQuorum + "/" + zkRootDir
      //createZkClientAndConnection
      //val zkClient = new
      // val zkClientAndConnection = ZkUtils.createZkClientAndConnection(zkUrl,sessionTimeOut,connectionTimeOut)
      //  ZkUtils.
    }
  }
}

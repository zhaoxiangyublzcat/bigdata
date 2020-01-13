package cn.blz.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @description
 * @className KafkaStreaming
 * @author Blzcat
 * @date 2020.01.13 11:29
 */
object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    val streamingContext = new StreamingContext(new SparkConf().setAppName("Test1").setMaster("local[*]"), Seconds(3))
    val kafkaDStream = KafkaUtils.createStream(streamingContext, "bio.cloudear.com:2181", "test1", Map("1" -> 1))
    kafkaDStream.flatMap(_._2.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

  }
}

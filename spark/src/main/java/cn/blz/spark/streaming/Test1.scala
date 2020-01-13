package cn.blz.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @description
 * @className Test1
 * @author Blzcat
 * @date 2020.01.13 10:41
 */
object Test1 {
  def main(args: Array[String]): Unit = {
    val streamingContext = new StreamingContext(new SparkConf().setAppName("Test1").setMaster("local[*]"), Seconds(3))
    //    val scoketDStream = streamingContext.socketTextStream("localhost", 404)
    //    scoketDStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()
    val textDStream = streamingContext.textFileStream("/a.txt")


    streamingContext.start()
    streamingContext.awaitTermination()
  }
}

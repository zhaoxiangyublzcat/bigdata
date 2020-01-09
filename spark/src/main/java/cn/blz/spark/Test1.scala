package cn.blz.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description
 * @className Test1
 * @author Blzcat
 * @date 2020.01.09 10:30
 */
object Test1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Test1").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile("hdfs://bio.cloudera.com:9000/tmp/a.txt")
    file.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      .saveAsTextFile("hdfs://bio.cloudera.com:9000/tmp/aout")

    sc.stop()
  }

}

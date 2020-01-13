package cn.blz.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description 单词统计
 * @className WordCount
 * @author Blzcat
 * @date 2020.01.09 10:30
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WordCount")
      .setMaster("local[*]")
    //      .setMaster("spark://bio.cloudera.com:7337")
    val sc = new SparkContext(sparkConf)


    val file = sc.textFile("/Users/blz/Downloads/a.txt")
    //    val file = sc.textFile("hdfs://bio.cloudera.com:8020/tmp/a.txt")
    file.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      .collect().foreach(println(_))
    sc.stop()


    //    val sparkSession = SparkSession.builder().appName("Test1")
    //      .master("spark://bio.cloudera.com:7337")
    //      //      .master("local[*]")
    //      .getOrCreate()
    ////    val file = sparkSession.read.textFile("/Users/blz/Downloads/a.txt")
    //    val file = sparkSession.read.textFile("hdfs://bio.cloudera.com:8020/tmp/a.txt")
    //    //添加隐式转换
    //    import sparkSession.implicits._
    //
    //    file.flatMap(_.split(" ")).groupByKey(_.toLowerCase).count().show()
  }

}

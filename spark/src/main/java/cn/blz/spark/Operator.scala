package cn.blz.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description 算子操作
 * @className Operator
 * @author Blzcat
 * @date 2020.01.10 16:59
 */
object Operator {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Test1"))

    val value = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8), 2)
    // 1. map
    //    val value2 = value.map(_ * 2)
    // 2. mapPartitions
    //    val value2 = value.mapPartitions(datas => {
    //      datas.map(_ * 2)
    //    })
    // 3. mapPartitionsWithIndex
    //    val value2 = value.mapPartitionsWithIndex {
    //      case (num, datas) => {
    //        datas
    //      }
    //    }
    // 3. flatMap
    //    val value2 = value.flatMap(datas => {
    //      datas.toString
    //    })
    value.collect()
    println(value.count())
    sc.stop()
  }
}

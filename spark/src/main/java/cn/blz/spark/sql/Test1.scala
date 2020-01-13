package cn.blz.spark.sql

import org.apache.spark.sql.SparkSession

/**
 * @description
 * @className Test1
 * @author Blzcat
 * @date 2020.01.12 16:06
 */
object Test1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test1")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    //    val df = spark.sparkContext.parallelize(List(("zs", 20), ("ls", 30), ("ww", 40))).map(x => {
    //      Persion(x._1, x._2)
    //    }).toDF()
    //    df.show()
    val ds = Seq(Persion("zs", 20), Persion("ls", 30), Persion("ww", 40)).toDS()

    ds.show()
  }

}

case class Persion(name: String, age: Int)
package cn.blz.spark.sql

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
 * @description
 * @className MysqlTest
 * @author Blzcat
 * @date 2020.01.12 20:31
 */
object MysqlTest {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","root")

    val spark = SparkSession.builder().appName("MysqlTest").master("local[*]").enableHiveSupport().getOrCreate()
    val gTable = spark.read.jdbc("jdbc:mysql://localhost:3306/bank_db", "GZCRM_CINFO", properties)
    gTable.write.jdbc("jdbc:mysql://localhost:3306/bank_db","GZCRM_CINFO_copy1",properties)
  }
}

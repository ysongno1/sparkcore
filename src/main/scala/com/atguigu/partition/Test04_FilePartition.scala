package com.atguigu.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test04_FilePartition {
  def main(args: Array[String]): Unit = {

    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    //数据读取位置计算是以偏移量为单位来进行计算的。
    val rdd: RDD[String] = sc.textFile("input1/1.txt", 3)

    rdd.saveAsTextFile("output3")

    //4.关闭sc
    sc.stop()

  }
}

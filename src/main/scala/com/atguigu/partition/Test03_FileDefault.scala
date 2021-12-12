package com.atguigu.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test03_FileDefault {
  def main(args: Array[String]): Unit = {

    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    // 默认填写的分区数为  (2 和 核数的最小值) 一般都是 2
    val rdd: RDD[String] = sc.textFile("input1/1.txt",2) //这里是最小分区数 并不是分区数
    rdd.saveAsTextFile("output2")

    //4.关闭sc
    sc.stop()


  }
}

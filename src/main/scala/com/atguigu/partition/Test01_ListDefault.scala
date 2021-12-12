package com.atguigu.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//默认分区测试

object Test01_ListDefault {
  def main(args: Array[String]): Unit = {

    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    //// 默认使用环境的核数(local[*]使用的是计算机的核数) 可以手动填写分区数
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    rdd.saveAsTextFile("output")

    //4.关闭sc
    sc.stop()

  }
}

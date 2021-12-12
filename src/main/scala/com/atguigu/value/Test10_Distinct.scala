package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test10_Distinct {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 1, 1, 2, 2, 2, 3, 4, 5), 2)

    intRDD.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)

    println("=======")

    val result: RDD[Int] = intRDD.distinct()//无参的话保留之前RDD的分区数

    //distinct会存在shuffle过程  数据按照哈希取模存放
    result.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)

    //4.关闭sc
    sc.stop()
  }
}

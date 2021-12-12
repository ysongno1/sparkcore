package com.atguigu.value

/*
   一对一和多对一不走shuffle
   一对多走shuffle
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test11_Coalesce {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 3)
    intRDD.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)

    println("==========")

    //缩减分区
    val result: RDD[Int] = intRDD.coalesce(2)
    result.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)

    println("==========")

    val result1: RDD[Int] = intRDD.coalesce(2,true)
    result1.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)

    //让线程睡眠观察4040界面
    Thread.sleep(300000)
    //4.关闭sc
    sc.stop()
  }
}

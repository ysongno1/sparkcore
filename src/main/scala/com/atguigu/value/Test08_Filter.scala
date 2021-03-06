package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test08_Filter {
  def main(args: Array[String]): Unit = {

    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5),2)

    intRDD.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)

    println("======")

    //过滤出所有偶数
    //使用filter 分区保持不变
    val result: RDD[Int] = intRDD.filter(_ % 2 == 0)

    result.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)



    //4.关闭sc
    sc.stop()

  }
}

package com.atguigu.dependency

/*
  走shuffle是分区器决定的 有个天然的优化
  如果前后两次执行RDD里的分区器是一样的 就不走Shuffle 中间不能穿插其他计算
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Test02_Dependency {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("input/1.txt")
    println(lineRDD.dependencies)
    println("------------------------------")

    val flatRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
    println(flatRDD.dependencies)
    println("------------------------------")

    val tupleRDD: RDD[(String, Int)] = flatRDD.map((_, 1))
    println(tupleRDD.dependencies)
    println("------------------------------")

    val result: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)
    println(result.dependencies)
    println("------------------------------")

    //    result.collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}

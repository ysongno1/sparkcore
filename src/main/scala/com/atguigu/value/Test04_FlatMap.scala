package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test04_FlatMap {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)


    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9)))

    // 扁平化操作
    val intrdd: RDD[Int] = rdd.flatMap(list => list)

    val tuplerdd: RDD[(String, Int)] = intrdd.map(i => ("我是", i))

    tuplerdd.collect().foreach(println)

    println("===================")

    // 将两步操作合并在一起
    rdd.flatMap(list => list.map(("我是",_)))

    //切分长字符串
    val strrdd: RDD[String] = sc.makeRDD(List("hello world", "hello spark"))

    val wordrdd: RDD[String] = strrdd.flatMap(s => s.split(" "))

    wordrdd.collect().foreach(println)

    //4.关闭sc
    sc.stop()
  }
}

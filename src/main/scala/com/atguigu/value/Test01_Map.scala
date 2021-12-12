package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test01_Map {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)


    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    rdd.saveAsTextFile("map1")

    val mapRdd: RDD[Int] = rdd.map(i => i * 2)
    val mapRdd1: RDD[Int] = rdd.map(_* 2)
    mapRdd1.saveAsTextFile("map2") //分区从map1到map2是不改变的 返回的是第一个爸爸妈妈(上一个RDD)的分区

    mapRdd.collect().foreach(println)

    //4.关闭sc
    sc.stop()
  }
}

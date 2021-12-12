package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test02_MapPartition {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5),2)

    /*
    区别是多了一层 mapPartition是对map的优化，性能更好 但可能OOM
      map是一次处理一个分区一个元素
      mapPartitions把一个分区拿出来当作一个集合，集合调用常用函数
     */
    //将RDD元素变成两倍
    //内部调用的map是集合常用函数中的 不是算子
    val rdd1: RDD[Int] = rdd.mapPartitions(list => list.map(_*2))
    val rdd2: RDD[Int] = rdd.mapPartitions(_.map(_*2))

    rdd1.collect().foreach(println)

    //4.关闭sc
    sc.stop()
  }
}

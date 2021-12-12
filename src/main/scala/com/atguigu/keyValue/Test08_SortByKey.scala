package com.atguigu.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test08_SortByKey {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

    //spark的排序是保证全局有序
    //默认升序  可以修改为降序
    val result: RDD[(Int, String)] = rdd.sortByKey()
    result.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)

    //4.关闭sc
    sc.stop()
  }
}

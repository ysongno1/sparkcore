package com.atguigu.createRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test01_FormList {

  //从集合中创建

  def main(args: Array[String]): Unit = {

    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    //3.从集合中创建RDD
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))

    rdd.collect().foreach(println)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    rdd1.collect().foreach(println)


    //4.关闭sc
    sc.stop()
  }

}

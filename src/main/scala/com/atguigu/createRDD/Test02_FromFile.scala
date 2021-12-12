package com.atguigu.createRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test02_FromFile {

  //从外部存储系统的数据集创建
  def main(args: Array[String]): Unit = {

    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    //从文件系统创建RDD
    val rdd: RDD[String] = sc.textFile("input")

    rdd.collect().foreach(println)

    val rdd1: RDD[String] = sc.textFile("hdfs://hadoop102:8020/input")

    rdd1.collect().foreach(println)

    //4.关闭sc
    sc.stop()

  }
}

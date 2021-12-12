package com.atguigu.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//不关注分区了 指定分区后每个分区数据的分配方式

object Test02_ListPartition {
  def main(args: Array[String]): Unit = {

    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    //分区的开始位置 = 分区号 * 数据总长度/分区总数
    //分区的结束位置 =（分区号 + 1）* 数据总长度/分区总数
    // 分区结果: 0->1   1->2   2->3,4
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 3)

    rdd.saveAsTextFile("output1")

    //4.关闭sc
    sc.stop()

  }
}

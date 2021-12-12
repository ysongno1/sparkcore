package com.atguigu.value

/*
  groupBy会存在shuffle过程
  shuffle：将不同的分区数据进行打乱重组的过程
  shuffle一定会落盘。可以在local模式下执行程序，通过4040看效果。

 */
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test06_GroupBy {
  def main(args: Array[String]): Unit = {

    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    val tupleRDD: RDD[(Int, Iterable[Int])] = intRDD.groupBy(i => i % 2,4)

    tupleRDD.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)

//    tupleRDD.saveAsTextFile("groupby")

    //4.关闭sc
    sc.stop()

  }
}
